// SPDX-License-Identifier: MIT

package expr

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Variadic is a Signature's Max when the function takes any number of
// arguments past its minimum.
const Variadic = -1

// ArgDomain is the set of types ONE argument position accepts.
//
// PostgreSQL resolves a call by its name AND its argument types, and reports a
// signature it does not have as `42883 function upper(integer) does not exist`
// — the identical code and shape it gives an unknown NAME. Measured on 17.11:
// `upper(1)`, `length(1)`, `lpad(1,3,'0')`, `split_part(1,'a',1)`,
// `starts_with(name,1)` and `replace(name,1,'x')` are each 42883, and so is
// every text-only function over a `bytea` operand — `upper(b)`, `lower(b)`,
// `btrim(b)`, `reverse(b)`, `replace(b,…)`, `left(b,1)`, `repeat(b,2)`,
// `split_part(b,…)`, `strpos(b,…)`, `lpad(b,…)`.
//
// Only three domains are needed for that, because only three answers exist:
// take anything, take text, take text or bytes.
type ArgDomain uint8

const (
	// ArgAny takes every type. It is the default for a position nothing
	// declares, and the honest answer for `concat`, `concat_ws` and `||`,
	// which PostgreSQL renders any operand through (`concat(1, name)` is
	// `1<name>`, measured).
	ArgAny ArgDomain = iota
	// ArgText takes TEXT and nothing else. A number is 42883 there, and so is
	// a BYTES value: PostgreSQL has no `upper(bytea)`, and answering one is
	// worse than answering a wrong code, because the client treats the rows as
	// the truth (#583).
	ArgText
	// ArgTextOrBytes takes text OR bytes, which is the accept-set of the
	// functions PostgreSQL DOES have over bytea: `length`, `octet_length`,
	// `bit_length`, `substring`, `md5`, `sha256`. A number is still 42883.
	ArgTextOrBytes
)

func (d ArgDomain) String() string {
	switch d {
	case ArgText:
		return "text"
	case ArgTextOrBytes:
		return "text or bytea"
	}
	return "any"
}

// Signature is what a registered function's NAME resolves to: how many
// arguments it takes, and what each position accepts.
//
// It exists because the registry recorded neither. Every entry is
// `func(args []any) any`, so a call with the wrong count could not be refused
// at bind time and each body read `args[i]` defensively: `semver_cmp('1.0.0')`
// answered NULL, `semver_major('1.2.3','x')` answered NULL and `upper('a','b')`
// answered 'A' with the extra argument dropped, where PostgreSQL answers 42883
// for all three (#1053). A filter over a mis-called function then evaluated
// NULL for every row and dropped the whole relation silently.
//
// The DOMAIN half is the same defect one layer down. FuncCall.EvalVec's `*Lit`
// branch built a constant vector for a numeric literal with no text arena and
// handed it to a STRING kernel, which indexed an absent offsets array: `SELECT
// concat(1, name)`, `starts_with(name, 1)`, `ends_with(name, 1)`,
// `contains(name, 1)` and `replace(name, 1, 'x')` were each XX000, an internal
// error, where PostgreSQL either renders the value or says 42883 (#1056). And
// the BYTES family reached those kernels through expr.toString, so every
// text-only function answered over bytea whatever Go string those bytes spell
// (#583). One bind-time check over declared argument types closes all three:
// the kernel is never reached with an operand it cannot read.
type Signature struct {
	// Min is the smallest legal argument count, Max the largest, or Variadic.
	Min, Max int
	// Args is the domain of each position; a position past the slice takes
	// Rest. Both empty means every position is ArgAny.
	Args []ArgDomain
	Rest ArgDomain
}

// domain reports what position i accepts.
func (s Signature) domain(i int) ArgDomain {
	if i < len(s.Args) {
		return s.Args[i]
	}
	return s.Rest
}

// Accepts reports whether n arguments satisfy this signature's arity. It is
// exported for the GRAMMAR-side closure: physical's rewrite gate parses every
// spelling this engine turns into a call the query did not write as one, and
// asks this about the call it produced (the round-1 review's B1).
func (s Signature) Accepts(n int) bool { return s.accepts(n) }

// accepts reports whether n arguments satisfy this signature's arity.
func (s Signature) accepts(n int) bool {
	if n < s.Min {
		return false
	}
	return s.Max == Variadic || n <= s.Max
}

// String renders the arity the way the sql-reference table spells it, for a
// gate's failure message.
func (s Signature) String() string {
	switch {
	case s.Max == Variadic:
		return fmt.Sprintf("%d or more", s.Min)
	case s.Min == s.Max:
		return fmt.Sprintf("exactly %d", s.Min)
	}
	return fmt.Sprintf("%d to %d", s.Min, s.Max)
}

// SignatureOf is the declared signature of a registered function. ok=false for
// a name the registry does not have — and, deliberately, for a UDF, whose
// arity is its own CREATE FUNCTION parameter list and is checked there.
func SignatureOf(name string) (Signature, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	if DefaultRegistry.IsUDF(n) {
		return Signature{}, false
	}
	s, ok := funcSignatures[n]
	return s, ok
}

// SignatureNames lists every name funcSignatures declares, for the gate that
// holds it and the registry equal in both directions.
func SignatureNames() []string {
	out := make([]string, 0, len(funcSignatures))
	for n := range funcSignatures {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// textDomains names, per function, the positions that are NOT ArgAny. It is
// kept apart from the arity table below so the two questions stay separable:
// every registered function has an ARITY (the table is closed, in both
// directions, and a gate says so), while a DOMAIN is declared only where
// PostgreSQL restricts one — the string family and the hash/encoding functions
// that read text.
//
// The default is ArgAny, which is the safe direction: the worst a missing
// declaration costs is the refusal PostgreSQL would have given, not a wrong
// one. What it must never do is claim ArgText for a position PostgreSQL takes
// a number in — that would refuse a statement the server answers.
//
// Measured on PostgreSQL 17.11 over a `text` column and a `bytea` column
// holding 'hi' and '\xfffe0041'. The split between the two text domains is the
// server's own: `length(bytea)`, `octet_length(bytea)`, `bit_length(bytea)`,
// `substring(bytea,…)`, `md5(bytea)` and `sha256(bytea)` all exist there and
// answer over bytes, while `upper(bytea)`, `lower(bytea)`, `btrim(bytea)`,
// `reverse(bytea)`, `replace(bytea,…)`, `starts_with(bytea,…)`,
// `strpos(bytea,…)`, `initcap(bytea)`, `lpad(bytea,…)`, `split_part(bytea,…)`,
// `left(bytea,…)` and `repeat(bytea,…)` do not.
var textDomains = map[string][]ArgDomain{
	// --- Text only: a number and a BYTES value are both 42883 ---
	"upper":            {ArgText},
	"lower":            {ArgText},
	"lcase":            {ArgText},
	"ucase":            {ArgText},
	"char_length":      {ArgText},
	"character_length": {ArgText},
	"trim":             {ArgText, ArgText},
	"ltrim":            {ArgText, ArgText},
	"rtrim":            {ArgText, ArgText},
	"reverse":          {ArgText},
	"replace":          {ArgText, ArgText, ArgText},
	"starts_with":      {ArgText, ArgText},
	"ends_with":        {ArgText, ArgText},
	"contains":         {ArgText, ArgText},
	"left":             {ArgText, ArgAny},
	"right":            {ArgText, ArgAny},
	"repeat":           {ArgText, ArgAny},
	"lpad":             {ArgText, ArgAny, ArgText},
	"rpad":             {ArgText, ArgAny, ArgText},
	"split_part":       {ArgText, ArgText, ArgAny},
	"translate":        {ArgText, ArgText, ArgText},
	"soundex":          {ArgText},
	"normalize":        {ArgText},
	"codepoint":        {ArgText},
	"regexp_like":      {ArgText, ArgText},
	"regexp_extract":   {ArgText, ArgText, ArgAny},
	"regexp_replace":   {ArgText, ArgText, ArgText},
	"regexp_count":     {ArgText, ArgText},
	"regexp_split":     {ArgText, ArgText},
	"split":            {ArgText, ArgText},
	// --- Text or bytes: the accept-set PostgreSQL has over bytea ---
	"length":       {ArgTextOrBytes},
	"len":          {ArgTextOrBytes},
	"strlen":       {ArgTextOrBytes},
	"octet_length": {ArgTextOrBytes},
	"bit_length":   {ArgTextOrBytes},
	"substr":       {ArgTextOrBytes, ArgAny, ArgAny},
	"substring":    {ArgTextOrBytes, ArgAny, ArgAny},
	"md5":          {ArgTextOrBytes},
	"sha256":       {ArgTextOrBytes},
	"sha512":       {ArgTextOrBytes},
	// strpos is POSITION's function spelling and the parser rewrites
	// `POSITION(sub IN s)` into it, so it has to take what `position(bytea in
	// bytea)` takes on the server even though `strpos(bytea, bytea)` itself is
	// 42883 there. One spelling answering where the other refuses is a
	// divergence this engine keeps deliberately, recorded in ADR-0012.
	"strpos":   {ArgTextOrBytes, ArgTextOrBytes},
	"position": {ArgTextOrBytes, ArgTextOrBytes},
	// The bridge: encode takes the BYTES side and decode the TEXT side, which
	// is the whole point of having both.
	"encode":   {ArgTextOrBytes, ArgText},
	"decode":   {ArgText, ArgText},
	"get_byte": {ArgTextOrBytes, ArgAny},
	"set_byte": {ArgTextOrBytes, ArgAny, ArgAny},
}

// funcSignatures is the ARITY of every registered scalar function, and it is
// closed in BOTH directions: TestEveryRegisteredFunctionDeclaresItsArity fails
// on a registry entry with no row here and on a row naming nothing registered.
//
// Each arity is its DOCUMENTED signature (docs/sql-reference.md's function
// tables, whose `f(a, b)` / `f(a [, b])` / `f(a, ...)` spellings say exactly
// this), reconciled against the implementation's own argument use; for the
// pg_catalog shims it is PostgreSQL's own, read out of that server's pg_proc,
// because the contract those exist to satisfy is what a client calls them
// with. Where wadjet implements FEWER overloads than PostgreSQL — `ltrim(s)`
// but not `ltrim(s, chars)`, `regexp_replace(s, p, r)` but not its four
// further forms — the declaration is what this engine HAS, and the missing
// overload is 42883, which is what the server answers for a signature it does
// not have either.
// THE ONE FAMILY WHOSE MISSING ARGUMENTS ARE NOT THIS TABLE'S BUSINESS. The
// TCP flag functions own a refusal for an empty NAME LIST —
// `22023 tcp_flag_mask requires at least one TCP flag name`, settled by #1018
// with its own binder half — and that refusal says which function and what it
// wanted, where a generic `function tcp_flag_mask() does not exist` says
// neither. Their minimum here is therefore the count that carries NO names, so
// the family's own refusal is the one a user reads.
var funcSignatures = map[string]Signature{
	"abs":                            {Min: 1, Max: 1},
	"acos":                           {Min: 1, Max: 1},
	"array_contains":                 {Min: 2, Max: 2},
	"array_join":                     {Min: 2, Max: 2},
	"array_length":                   {Min: 1, Max: 2},
	"array_max":                      {Min: 1, Max: 1},
	"array_min":                      {Min: 1, Max: 1},
	"array_to_string":                {Min: 2, Max: 3},
	"ascii":                          {Min: 1, Max: 1},
	"asin":                           {Min: 1, Max: 1},
	"at_timezone":                    {Min: 2, Max: 2},
	"atan":                           {Min: 1, Max: 1},
	"atan2":                          {Min: 2, Max: 2},
	"bit_count":                      {Min: 1, Max: 1},
	"bit_length":                     {Min: 1, Max: 1},
	"bitwise_and":                    {Min: 2, Max: 2},
	"bitwise_arithmetic_shift_right": {Min: 2, Max: 2},
	"bitwise_left_shift":             {Min: 2, Max: 2},
	"bitwise_not":                    {Min: 1, Max: 1},
	"bitwise_or":                     {Min: 2, Max: 2},
	"bitwise_right_shift":            {Min: 2, Max: 2},
	"bitwise_xor":                    {Min: 2, Max: 2},
	"broadcast_address":              {Min: 1, Max: 1},
	"cardinality":                    {Min: 1, Max: 1},
	"cast_float":                     {Min: 1, Max: 1},
	"cast_int":                       {Min: 1, Max: 1},
	"cast_string":                    {Min: 1, Max: 1},
	"cbrt":                           {Min: 1, Max: 1},
	"ceil":                           {Min: 1, Max: 1},
	"ceiling":                        {Min: 1, Max: 1},
	"char_length":                    {Min: 1, Max: 1},
	"character_length":               {Min: 1, Max: 1},
	"chr":                            {Min: 1, Max: 1},
	"cidr_contains":                  {Min: 2, Max: 2},
	"cidr_overlap":                   {Min: 2, Max: 2},
	"cidr_to_range":                  {Min: 1, Max: 1},
	"coalesce":                       {Min: 1, Max: Variadic},
	"codepoint":                      {Min: 1, Max: 1},
	"col_description":                {Min: 2, Max: 2},
	"concat":                         {Min: 1, Max: Variadic},
	"concat_ws":                      {Min: 2, Max: Variadic},
	"contains":                       {Min: 2, Max: 2},
	"cos":                            {Min: 1, Max: 1},
	"cosine_similarity":              {Min: 2, Max: 2},
	"crc32":                          {Min: 1, Max: 1},
	"current_catalog":                {Min: 0, Max: 0},
	"current_database":               {Min: 0, Max: 0},
	"current_date":                   {Min: 0, Max: 0},
	"current_query":                  {Min: 0, Max: 0},
	"current_role":                   {Min: 0, Max: 0},
	"current_schema":                 {Min: 0, Max: 0},
	"current_schemas":                {Min: 1, Max: 1},
	"current_setting":                {Min: 1, Max: 2},
	"current_timestamp":              {Min: 0, Max: 0},
	"current_user":                   {Min: 0, Max: 0},
	"date_add":                       {Min: 2, Max: 2},
	"date_diff":                      {Min: 2, Max: 2},
	"date_format":                    {Min: 2, Max: 2},
	"date_parse":                     {Min: 2, Max: 2},
	"date_part":                      {Min: 2, Max: 2},
	"date_sub":                       {Min: 2, Max: 2},
	"date_trunc":                     {Min: 2, Max: 2},
	"day":                            {Min: 1, Max: 1},
	"day_of_week":                    {Min: 1, Max: 1},
	"day_of_year":                    {Min: 1, Max: 1},
	"degrees":                        {Min: 1, Max: 1},
	"dns_answer_count":               {Min: 1, Max: 1},
	"dns_is_response":                {Min: 1, Max: 1},
	"dns_query_name":                 {Min: 1, Max: 1},
	"dns_query_type":                 {Min: 1, Max: 1},
	"dns_question_count":             {Min: 1, Max: 1},
	"dns_response_code":              {Min: 1, Max: 1},
	"dns_transaction_id":             {Min: 1, Max: 1},
	"domain_depth":                   {Min: 1, Max: 1},
	"dot_product":                    {Min: 2, Max: 2},
	"e":                              {Min: 0, Max: 0},
	"encode":                         {Min: 2, Max: 2},
	"decode":                         {Min: 2, Max: 2},
	"get_byte":                       {Min: 2, Max: 2},
	"set_byte":                       {Min: 3, Max: 3},
	"embed":                          {Min: 1, Max: 1},
	"embed_model":                    {Min: 0, Max: 0},
	"embed_dim":                      {Min: 0, Max: 0},
	"element_at":                     {Min: 2, Max: 2},
	"ends_with":                      {Min: 2, Max: 2},
	"entropy":                        {Min: 1, Max: 1},
	"epoch":                          {Min: 1, Max: 1},
	"ether_type":                     {Min: 1, Max: 1},
	"exp":                            {Min: 1, Max: 1},
	"extract":                        {Min: 2, Max: 2},
	"floor":                          {Min: 1, Max: 1},
	"flow_direction":                 {Min: 2, Max: 2},
	"format":                         {Min: 1, Max: Variadic},
	"format_bytes":                   {Min: 1, Max: 2},
	"format_number":                  {Min: 1, Max: 2},
	"format_rate":                    {Min: 1, Max: 2},
	"from_base":                      {Min: 2, Max: 2},
	"from_base32":                    {Min: 1, Max: 1},
	"from_base64":                    {Min: 1, Max: 1},
	"from_hex":                       {Min: 1, Max: 1},
	"from_iso8601_date":              {Min: 1, Max: 1},
	"from_iso8601_timestamp":         {Min: 1, Max: 1},
	"from_unixtime":                  {Min: 1, Max: 1},
	"from_utf8":                      {Min: 1, Max: 1},
	"geoip_asn":                      {Min: 1, Max: 1},
	"geoip_city":                     {Min: 1, Max: 1},
	"geoip_continent":                {Min: 1, Max: 1},
	"geoip_country":                  {Min: 1, Max: 1},
	"geoip_country_name":             {Min: 1, Max: 1},
	"geoip_latitude":                 {Min: 1, Max: 1},
	"geoip_longitude":                {Min: 1, Max: 1},
	"geoip_org":                      {Min: 1, Max: 1},
	"geoip_postal_code":              {Min: 1, Max: 1},
	"geoip_subdivision":              {Min: 1, Max: 1},
	"geoip_timezone":                 {Min: 1, Max: 1},
	"greatest":                       {Min: 1, Max: Variadic},
	"hamming_distance":               {Min: 2, Max: 2},
	"has_column_privilege":           {Min: 3, Max: 4},
	"has_database_privilege":         {Min: 2, Max: 3},
	"has_function_privilege":         {Min: 2, Max: 3},
	"has_schema_privilege":           {Min: 2, Max: 3},
	"has_sequence_privilege":         {Min: 2, Max: 3},
	"has_table_privilege":            {Min: 2, Max: 3},
	"has_tablespace_privilege":       {Min: 2, Max: 3},
	"has_tcp_flag":                   {Min: 1, Max: 2},
	"hmac_sha256":                    {Min: 2, Max: 2},
	"hmac_sha512":                    {Min: 2, Max: 2},
	"hosts_in_cidr":                  {Min: 1, Max: 1},
	"hour":                           {Min: 1, Max: 1},
	"http_content_length":            {Min: 1, Max: 1},
	"http_content_type":              {Min: 1, Max: 1},
	"http_header":                    {Min: 2, Max: 2},
	"http_host":                      {Min: 1, Max: 1},
	"http_method":                    {Min: 1, Max: 1},
	"http_path":                      {Min: 1, Max: 1},
	"http_status_class":              {Min: 1, Max: 1},
	"http_status_code":               {Min: 1, Max: 1},
	"http_user_agent":                {Min: 1, Max: 1},
	"http_version":                   {Min: 1, Max: 1},
	"human_readable_seconds":         {Min: 1, Max: 1},
	"icmp_code":                      {Min: 1, Max: 1},
	"icmp_code_name":                 {Min: 2, Max: 2},
	"icmp_parse":                     {Min: 1, Max: 1},
	"icmp_type":                      {Min: 1, Max: 1},
	"icmp_type_name":                 {Min: 1, Max: 1},
	"if":                             {Min: 3, Max: 3},
	"ifnull":                         {Min: 2, Max: 2},
	"inet_client_addr":               {Min: 0, Max: 0},
	"inet_server_addr":               {Min: 0, Max: 0},
	"infinity":                       {Min: 0, Max: 0},
	"int_to_ip":                      {Min: 1, Max: 1},
	"ip_add":                         {Min: 2, Max: 2},
	"ip_between":                     {Min: 3, Max: 3},
	"ip_diff":                        {Min: 2, Max: 2},
	"ip_dscp":                        {Min: 1, Max: 1},
	"ip_header_length":               {Min: 1, Max: 1},
	"ip_in_range":                    {Min: 3, Max: 3},
	"ip_netmask":                     {Min: 1, Max: 1},
	"ip_subnet":                      {Min: 1, Max: 1},
	"ip_subtract":                    {Min: 2, Max: 2},
	"ip_to_hex":                      {Min: 1, Max: 1},
	"ip_to_int":                      {Min: 1, Max: 1},
	"ip_to_string":                   {Min: 1, Max: 1},
	"ip_total_length":                {Min: 1, Max: 1},
	"ip_ttl":                         {Min: 1, Max: 1},
	"ip_version":                     {Min: 1, Max: 1},
	"ipv6_compress":                  {Min: 1, Max: 1},
	"ipv6_expand":                    {Min: 1, Max: 1},
	"ipv6_scope":                     {Min: 1, Max: 1},
	"ipv6_to_eui64":                  {Min: 1, Max: 1},
	"is_6to4":                        {Min: 1, Max: 1},
	"is_ephemeral_port":              {Min: 1, Max: 1},
	"is_finite":                      {Min: 1, Max: 1},
	"is_http_request":                {Min: 1, Max: 1},
	"is_http_response":               {Min: 1, Max: 1},
	"is_icmp_echo":                   {Min: 1, Max: 1},
	"is_infinite":                    {Min: 1, Max: 1},
	"is_ipv4":                        {Min: 1, Max: 1},
	"is_ipv6":                        {Min: 1, Max: 1},
	"is_link_local_ip":               {Min: 1, Max: 1},
	"is_loopback_ip":                 {Min: 1, Max: 1},
	"is_multicast_ip":                {Min: 1, Max: 1},
	"is_nan":                         {Min: 1, Max: 1},
	"is_private_ip":                  {Min: 1, Max: 1},
	"is_registered_port":             {Min: 1, Max: 1},
	"is_reserved_ip":                 {Min: 1, Max: 1},
	"is_tcp_handshake":               {Min: 1, Max: 1},
	"is_tcp_reset":                   {Min: 1, Max: 1},
	"is_teredo":                      {Min: 1, Max: 1},
	"is_tls_client_hello":            {Min: 1, Max: 1},
	"is_well_known_port":             {Min: 1, Max: 1},
	"ja3_fingerprint":                {Min: 1, Max: 1},
	"ja3_string":                     {Min: 1, Max: 1},
	"ja3s_fingerprint":               {Min: 1, Max: 1},
	"ja3s_string":                    {Min: 1, Max: 1},
	"json_array_length":              {Min: 1, Max: 1},
	"json_extract":                   {Min: 2, Max: 2},
	"json_extract_scalar":            {Min: 2, Max: 2},
	"json_valid":                     {Min: 1, Max: 1},
	"l2_distance":                    {Min: 2, Max: 2},
	"last_day_of_month":              {Min: 1, Max: 1},
	"lcase":                          {Min: 1, Max: 1},
	"least":                          {Min: 1, Max: Variadic},
	"left":                           {Min: 2, Max: 2},
	"len":                            {Min: 1, Max: 1},
	"length":                         {Min: 1, Max: 1},
	"levenshtein_distance":           {Min: 2, Max: 2},
	"ln":                             {Min: 1, Max: 1},
	"log":                            {Min: 1, Max: 2},
	"log10":                          {Min: 1, Max: 1},
	"log2":                           {Min: 1, Max: 1},
	"lower":                          {Min: 1, Max: 1},
	"lpad":                           {Min: 2, Max: 3},
	"ltrim":                          {Min: 1, Max: 2},
	"mac_format":                     {Min: 1, Max: 2},
	"mac_is_local":                   {Min: 1, Max: 1},
	"mac_is_unicast":                 {Min: 1, Max: 1},
	"mac_to_string":                  {Min: 1, Max: 1},
	"mac_vendor_oui":                 {Min: 1, Max: 1},
	"map_entries":                    {Min: 1, Max: 1},
	"map_from_entries":               {Min: 1, Max: 1},
	"map_keys":                       {Min: 1, Max: 1},
	"map_values":                     {Min: 1, Max: 1},
	"mask_ip":                        {Min: 2, Max: 2},
	"md5":                            {Min: 1, Max: 1},
	"minute":                         {Min: 1, Max: 1},
	"mod":                            {Min: 2, Max: 2},
	"month":                          {Min: 1, Max: 1},
	"murmur3":                        {Min: 1, Max: 1},
	"nan":                            {Min: 0, Max: 0},
	"network_address":                {Min: 1, Max: 1},
	"normalize":                      {Min: 1, Max: 1},
	"now":                            {Min: 0, Max: 0},
	"nullif":                         {Min: 2, Max: 2},
	"obj_description":                {Min: 1, Max: 2},
	"octet_length":                   {Min: 1, Max: 1},
	"parse_bytes":                    {Min: 1, Max: 1},
	"parse_rate":                     {Min: 1, Max: 1},
	"payload_contains":               {Min: 2, Max: 2},
	"payload_entropy":                {Min: 1, Max: 1},
	"payload_hex_dump":               {Min: 1, Max: 2},
	"payload_length":                 {Min: 1, Max: 1},
	"payload_matches":                {Min: 2, Max: 2},
	"payload_offset":                 {Min: 3, Max: 3},
	"pg_backend_pid":                 {Min: 0, Max: 0},
	"pg_client_encoding":             {Min: 0, Max: 0},
	"pg_collation_is_visible":        {Min: 1, Max: 1},
	"pg_conf_load_time":              {Min: 0, Max: 0},
	"pg_current_xact_id":             {Min: 0, Max: 0},
	"pg_encoding_to_char":            {Min: 1, Max: 1},
	"pg_function_is_visible":         {Min: 1, Max: 1},
	"pg_has_role":                    {Min: 2, Max: 3},
	"pg_is_in_recovery":              {Min: 0, Max: 0},
	"pg_my_temp_schema":              {Min: 0, Max: 0},
	"pg_opclass_is_visible":          {Min: 1, Max: 1},
	"pg_operator_is_visible":         {Min: 1, Max: 1},
	"pg_postmaster_start_time":       {Min: 0, Max: 0},
	"pg_table_is_visible":            {Min: 1, Max: 1},
	"pg_type_is_visible":             {Min: 1, Max: 1},
	"pi":                             {Min: 0, Max: 0},
	"port_class":                     {Min: 1, Max: 1},
	"port_name":                      {Min: 1, Max: 1},
	"position":                       {Min: 2, Max: 2},
	"pow":                            {Min: 2, Max: 2},
	"power":                          {Min: 2, Max: 2},
	"prefix_length":                  {Min: 1, Max: 1},
	"protocol_name":                  {Min: 1, Max: 1},
	"protocol_number":                {Min: 1, Max: 1},
	"quarter":                        {Min: 1, Max: 1},
	"quote_ident":                    {Min: 1, Max: 1},
	"quote_literal":                  {Min: 1, Max: 1},
	"quote_nullable":                 {Min: 1, Max: 1},
	"radians":                        {Min: 1, Max: 1},
	"rand":                           {Min: 0, Max: 0},
	"random":                         {Min: 0, Max: 0},
	"regexp_count":                   {Min: 2, Max: 2},
	"regexp_extract":                 {Min: 2, Max: 3},
	"regexp_extract_all":             {Min: 2, Max: 2},
	"regexp_like":                    {Min: 2, Max: 2},
	"regexp_replace":                 {Min: 3, Max: 3},
	"regexp_split":                   {Min: 2, Max: 2},
	"registered_domain":              {Min: 1, Max: 1},
	"repeat":                         {Min: 2, Max: 2},
	"replace":                        {Min: 3, Max: 3},
	"reverse":                        {Min: 1, Max: 1},
	"reverse_dns":                    {Min: 1, Max: 1},
	"right":                          {Min: 2, Max: 2},
	"round":                          {Min: 1, Max: 2},
	"round_half_even":                {Min: 1, Max: 2},
	"row_field":                      {Min: 2, Max: 2},
	"rpad":                           {Min: 2, Max: 3},
	"rtrim":                          {Min: 1, Max: 2},
	"same_subnet":                    {Min: 3, Max: 3},
	"second":                         {Min: 1, Max: 1},
	"semver_build":                   {Min: 1, Max: 1},
	"semver_cmp":                     {Min: 2, Max: 2},
	"semver_major":                   {Min: 1, Max: 1},
	"semver_minor":                   {Min: 1, Max: 1},
	"semver_normalize":               {Min: 1, Max: 1},
	"semver_normalize_strict":        {Min: 1, Max: 1},
	"semver_parse":                   {Min: 1, Max: 1},
	"semver_parse_strict":            {Min: 1, Max: 1},
	"semver_patch":                   {Min: 1, Max: 1},
	"semver_prerelease":              {Min: 1, Max: 1},
	"semver_satisfies":               {Min: 2, Max: 2},
	"semver_sort_key":                {Min: 1, Max: 1},
	"semver_valid":                   {Min: 1, Max: 1},
	"session_user":                   {Min: 0, Max: 0},
	"set_config":                     {Min: 3, Max: 3},
	"sha1":                           {Min: 1, Max: 1},
	"sha256":                         {Min: 1, Max: 1},
	"sha512":                         {Min: 1, Max: 1},
	"shobj_description":              {Min: 2, Max: 2},
	"sign":                           {Min: 1, Max: 1},
	"sin":                            {Min: 1, Max: 1},
	"sixto4_gateway":                 {Min: 1, Max: 1},
	"soundex":                        {Min: 1, Max: 1},
	"split":                          {Min: 2, Max: 2},
	"split_part":                     {Min: 3, Max: 3},
	"sqrt":                           {Min: 1, Max: 1},
	"starts_with":                    {Min: 2, Max: 2},
	"strlen":                         {Min: 1, Max: 1},
	"strpos":                         {Min: 2, Max: 2},
	"struct_field":                   {Min: 2, Max: 2},
	"subdomain":                      {Min: 1, Max: 1},
	"substr":                         {Min: 2, Max: 3},
	"substring":                      {Min: 2, Max: 3},
	"tan":                            {Min: 1, Max: 1},
	"tcp_flag_mask":                  {Min: 0, Max: Variadic},
	"tcp_flags":                      {Min: 1, Max: 1},
	"tcp_flags_from_string":          {Min: 1, Max: 1},
	"tcp_flags_has_all":              {Min: 1, Max: Variadic},
	"tcp_flags_has_any":              {Min: 1, Max: Variadic},
	"tcp_flags_has_none":             {Min: 1, Max: Variadic},
	"tcp_flags_text":                 {Min: 1, Max: 1},
	"tcp_flags_to_string":            {Min: 1, Max: 1},
	"tcp_session_id":                 {Min: 5, Max: 5},
	"teredo_client":                  {Min: 1, Max: 1},
	"teredo_server":                  {Min: 1, Max: 1},
	"time_bucket":                    {Min: 2, Max: 3},
	"timezone":                       {Min: 2, Max: 2},
	"timezone_hour":                  {Min: 1, Max: 1},
	"timezone_minute":                {Min: 1, Max: 1},
	"tld":                            {Min: 1, Max: 1},
	"tls_handshake_type":             {Min: 1, Max: 1},
	"tls_record_type":                {Min: 1, Max: 1},
	"tls_sni":                        {Min: 1, Max: 1},
	"tls_version":                    {Min: 1, Max: 1},
	"to_base":                        {Min: 2, Max: 2},
	"to_base32":                      {Min: 1, Max: 1},
	"to_base64":                      {Min: 1, Max: 1},
	"to_date":                        {Min: 1, Max: 1},
	"to_hex":                         {Min: 1, Max: 1},
	"to_iso8601":                     {Min: 1, Max: 1},
	"to_milliseconds":                {Min: 1, Max: 1},
	"to_unixtime":                    {Min: 1, Max: 1},
	"to_utf8":                        {Min: 1, Max: 1},
	"translate":                      {Min: 3, Max: 3},
	"trim":                           {Min: 1, Max: 2},
	"trunc":                          {Min: 1, Max: 2},
	"truncate":                       {Min: 1, Max: 2},
	"txid_current":                   {Min: 0, Max: 0},
	"typeof":                         {Min: 1, Max: 1},
	"ucase":                          {Min: 1, Max: 1},
	"upper":                          {Min: 1, Max: 1},
	"url_decode":                     {Min: 1, Max: 1},
	"url_encode":                     {Min: 1, Max: 1},
	"url_extract_host":               {Min: 1, Max: 1},
	"url_extract_parameter":          {Min: 2, Max: 2},
	"url_extract_path":               {Min: 1, Max: 1},
	"url_extract_port":               {Min: 1, Max: 1},
	"url_extract_protocol":           {Min: 1, Max: 1},
	"url_extract_query":              {Min: 1, Max: 1},
	"user":                           {Min: 0, Max: 0},
	"uuid":                           {Min: 0, Max: 0},
	"uuid_to_string":                 {Min: 1, Max: 1},
	"uuid_version":                   {Min: 1, Max: 1},
	"vector_dims":                    {Min: 1, Max: 1},
	"vector_norm":                    {Min: 1, Max: 1},
	"version":                        {Min: 0, Max: 0},
	"vlan_id":                        {Min: 1, Max: 1},
	"week":                           {Min: 1, Max: 1},
	"width_bucket":                   {Min: 4, Max: 4},
	"xxhash64":                       {Min: 1, Max: 1},
	"year":                           {Min: 1, Max: 1},
	ConcatOpFunc:                     {Min: 2, Max: 2},
}

func init() {
	// The domains are folded into the arity table at startup rather than
	// written into each row, so the two tables stay separately reviewable and
	// a domain cannot be declared for a function that is not registered:
	// TestEveryRegisteredFunctionDeclaresItsArity closes funcSignatures
	// against the registry, and this loop closes textDomains against
	// funcSignatures.
	for name, doms := range textDomains {
		s, ok := funcSignatures[name]
		if !ok {
			panic(fmt.Sprintf("expr: textDomains declares %q, which has no signature", name))
		}
		s.Args = doms
		funcSignatures[name] = s
	}
}

// WrongSignatureError names a call the registry cannot resolve because of its
// ARGUMENTS rather than its name — the wrong number of them, or one whose type
// no overload takes.
//
// It is a distinct type from UnknownFuncError for the same reason that one is
// distinct: fatal.go's non-fallback predicate has to recognize it, or the
// physical planner's forgiving projection fallback converts "function
// upper(unknown, unknown) does not exist" into "column \"upper(a, b)\" does not
// exist in the input schema", which sends the reader looking for a column.
type WrongSignatureError struct {
	Name string
	Args []string
}

func (e *WrongSignatureError) Error() string {
	return fmt.Sprintf("function %s(%s) does not exist", e.Name, strings.Join(e.Args, ", "))
}

// SQLState returns PostgreSQL's undefined_function code, the same one it gives
// for a call whose argument types match no overload.
func (e *WrongSignatureError) SQLState() string { return "42883" }

// IsWrongSignature reports whether err is, or wraps, a WrongSignatureError.
func IsWrongSignature(err error) bool {
	var wse *WrongSignatureError
	return errors.As(err, &wse)
}

// RefuseUnresolvableCall refuses a call whose ARGUMENTS no overload of its name
// takes, with PostgreSQL's own 42883 and its message shape:
//
//	function upper(unknown, unknown) does not exist
//	function upper(integer) does not exist
//	function replace(text, integer, unknown) does not exist
//
// decl resolves one argument node's DECLARED type; Undecided means "this layer
// cannot say", and an undecided argument is never refused. That asymmetry is
// deliberate and is the same one RefuseInvalidFixedRowField makes: a refusal
// built on a guess would refuse statements the server answers, while a missed
// refusal costs only the answer this engine already gives.
//
// The ARITY half asks nothing of the schema, so it decides for every call. The
// DOMAIN half decides for every argument whose type is settled — a literal
// always, a column wherever the caller has the declarations.
func RefuseUnresolvableCall(n *plansql.FuncCallNode, decl func(plansql.Node) (DeclType, Confidence)) error {
	if n == nil || n.Star {
		// `count(*)` and its siblings are the aggregate layer's, and a star
		// is not an argument list this can count.
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(n.Name))
	sig, ok := SignatureOf(name)
	if !ok {
		return nil
	}
	types := callArgTypeNames(n.Args, decl)
	if !sig.accepts(len(n.Args)) {
		return &WrongSignatureError{Name: name, Args: types}
	}
	for i, arg := range n.Args {
		d := sig.domain(i)
		if d == ArgAny {
			continue
		}
		t, c := argDeclType(arg, decl)
		if c != Decided {
			continue
		}
		if _, isLit := arg.(*plansql.Lit); isLit {
			// A LITERAL's type is SYNTACTIC — the query wrote it — so this
			// refuses on it with no schema and no guess. It is also the shape
			// PostgreSQL's own message describes: `upper(1)` is `function
			// upper(integer) does not exist` there, and here it was XX000,
			// an internal error that tells the client nothing about its own
			// statement (#1056).
			if literalFitsDomain(d, t.ID) {
				continue
			}
			return &WrongSignatureError{Name: name, Args: types}
		}
		// A COLUMN or a derived value. Only ONE declared type is refused
		// here, and deliberately: BYTES in a TEXT-ONLY position.
		//
		// PostgreSQL has no `upper(bytea)` and answering one is the wrong
		// VALUE — the bytes reinterpreted as whatever Go string they spell,
		// with #570's embedded-NUL hazard behind it (#583). Every OTHER
		// non-text type in a text position is this engine's recorded
		// SUPERSET, not a defect: a DATE, an IPv4, a MAC, a TIMESTAMP and an
		// integer column are RENDERED as their text before a string function
		// reads them (stringInputFuncs, #273/#500/#544/#568), which is what
		// makes `UPPER(mac_col)` and `SUBSTR(date_col, 1, 4)` the network
		// analytics this engine is for. PostgreSQL refuses those and this
		// engine answers them; ADR-0012 records that class, and narrowing it
		// here would delete a documented feature rather than close a defect.
		if d == ArgText && t.ID == batch.TypeBytes {
			return &WrongSignatureError{Name: name, Args: types}
		}
	}
	return nil
}

// literalFitsDomain reports whether a LITERAL of this declared type may stand
// in a position of this domain. SQL's `unknown` — a bare quoted literal, and
// NULL — fits everywhere, because that is what PostgreSQL does with it: it
// takes the type the call demands. A number and a boolean fit neither text
// domain.
func literalFitsDomain(d ArgDomain, t batch.TypeID) bool {
	switch d {
	case ArgText, ArgTextOrBytes:
		return t == batch.TypeString
	}
	return true
}

// argDeclType is decl with the two shapes this layer can decide on its own
// first: a quoted literal is SQL's `unknown` and names no type at all, and a
// numeric or boolean literal names one without any schema.
func argDeclType(arg plansql.Node, decl func(plansql.Node) (DeclType, Confidence)) (DeclType, Confidence) {
	if lit, ok := arg.(*plansql.Lit); ok {
		switch lit.Kind {
		case plansql.LitString:
			// `unknown`: it takes the type its position demands.
			return DeclType{Untyped: true}, Undecided
		case plansql.LitNull:
			return DeclType{Untyped: true}, Undecided
		case plansql.LitNumber:
			// PostgreSQL types a bare `1` integer and a bare `1.5` numeric,
			// and refuses both in a text position — the SPELLING is what says
			// which name the message carries (`upper(1)` says integer,
			// `lower(1.5)` says numeric, measured).
			if strings.ContainsAny(lit.Value, ".eE") {
				return Decl(batch.TypeDecimal), Decided
			}
			return Decl(batch.TypeInt64), Decided
		case plansql.LitBool:
			return Decl(batch.TypeBool), Decided
		}
	}
	if decl == nil {
		return DeclType{}, Undecided
	}
	return decl(arg)
}

// callArgTypeNames renders a call's argument types the way PostgreSQL's 42883
// message does: `unknown` for a bare quoted literal or NULL, the type's own
// PostgreSQL name otherwise.
func callArgTypeNames(args []plansql.Node, decl func(plansql.Node) (DeclType, Confidence)) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		t, c := argDeclType(a, decl)
		if c != Decided {
			out = append(out, "unknown")
			continue
		}
		out = append(out, pgTypeNameOf(t.ID))
	}
	return out
}

// pgTypeNameOf is the name PostgreSQL puts in a 42883 message for a type, so a
// client reading the error can look it up in pg_type. The wadjet-native
// network types name themselves; everything else uses the server's spelling.
func pgTypeNameOf(t batch.TypeID) string {
	switch t {
	case batch.TypeString:
		return "text"
	case batch.TypeBytes:
		return "bytea"
	case batch.TypeInt32:
		return "integer"
	case batch.TypeInt64:
		return "bigint"
	case batch.TypeFloat32:
		return "real"
	case batch.TypeFloat64:
		return "double precision"
	case batch.TypeDecimal:
		return "numeric"
	case batch.TypeBool:
		return "boolean"
	case batch.TypeTimestamp:
		return "timestamp"
	case batch.TypeDate:
		return "date"
	case batch.TypeIPv4, batch.TypeIPv6:
		return "inet"
	case batch.TypeCIDR:
		return "inet"
	case batch.TypeMAC:
		return "macaddr"
	case batch.TypeUUID:
		return "uuid"
	}
	return strings.ToLower(parquet.TypeID(t).String())
}

// HasVecKernel reports whether a function has a VECTORIZED implementation. It
// is exported for the gate that walks every such kernel with a literal in each
// argument position — the one #1056 asks for, which has to be generated from
// the registry rather than listed, or a kernel added later is uncovered.
func HasVecKernel(name string) bool { return DefaultRegistry.LookupVec(name) != nil }

// DomainAt is the declared domain of one argument position, for the same gate.
func (s Signature) DomainAt(i int) ArgDomain { return s.domain(i) }
