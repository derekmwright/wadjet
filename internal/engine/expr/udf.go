package expr

import (
	"fmt"
	"strings"
	"sync"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/derekmwright/caelum/internal/engine/batch"
)

// UDFDef defines a user-defined function.
type UDFDef struct {
	Name   string   // function name (lowercase)
	Params []string // parameter names (lowercase)
	Body   string   // SQL expression body (e.g. "param1 * 2 + param2")
	Owner  string   // who created this function (empty = system/unowned)
	Locked bool     // if true, only the owner (or admin) can modify/drop
}

// ParamRef is an expression node that references a UDF parameter by index.
// During UDF evaluation, the caller binds argument values into a shared
// args slice; ParamRef reads from that slice at zero allocation cost.
type ParamRef struct {
	Index int
	args  *[]any // pointer to the shared args slice (set at compile time)
}

func (e *ParamRef) Eval(_ *batch.RecordBatch, _ int) any {
	if e.args == nil || e.Index >= len(*e.args) {
		return nil
	}
	return (*e.args)[e.Index]
}

// UDFCall evaluates a user-defined function by binding arguments, then
// evaluating the compiled body expression.
type UDFCall struct {
	Name     string
	ArgExprs []Expr // caller-supplied argument expressions
	Body     Expr   // compiled UDF body with ParamRef nodes
	args     []any  // shared args buffer (bound to ParamRef nodes)
	argsPtr  *[]any // pointer shared with all ParamRef nodes
}

func (e *UDFCall) Eval(b *batch.RecordBatch, row int) any {
	// Lazily allocate args buffer
	if e.args == nil {
		e.args = make([]any, len(e.ArgExprs))
		e.argsPtr = &e.args
	}
	// Evaluate caller arguments into the shared buffer
	for i, arg := range e.ArgExprs {
		e.args[i] = arg.Eval(b, row)
	}
	// Evaluate the body — ParamRef nodes read from e.args via argsPtr
	return e.Body.Eval(b, row)
}

// UDFStore holds compiled UDF definitions for use by the expression engine.
// Thread-safe for concurrent reads and writes.
type UDFStore struct {
	mu   sync.RWMutex
	udfs map[string]*compiledUDF
}

type compiledUDF struct {
	def  UDFDef
	body Expr   // compiled body expression (template — cloned per call site)
	deps []string // names of other UDFs this one depends on
}

// NewUDFStore creates a new empty UDF store.
func NewUDFStore() *UDFStore {
	return &UDFStore{udfs: make(map[string]*compiledUDF)}
}

// DefaultUDFs is the global UDF store.
var DefaultUDFs = NewUDFStore()

// Register compiles and registers a UDF. Returns an error if the body
// cannot be parsed, references unknown functions/UDFs, or the caller lacks
// permission to create/replace the function.
//
// Permission rules:
//   - Builtin functions (registered via RegisterFunc) cannot be overwritten
//   - If a UDF already exists and is Locked, only the original Owner or
//     a caller with isAdmin=true can replace it
//   - If a UDF already exists and is not Locked, any caller can replace it
func (s *UDFStore) Register(def UDFDef, isAdmin bool) error {
	def.Name = strings.ToLower(def.Name)
	for i := range def.Params {
		def.Params[i] = strings.ToLower(def.Params[i])
	}

	// Prevent overwriting builtin functions
	if isBuiltinFunc(def.Name) {
		return fmt.Errorf("cannot overwrite builtin function %q", def.Name)
	}

	// Check ownership if replacing an existing UDF
	s.mu.RLock()
	if existing, ok := s.udfs[def.Name]; ok {
		if existing.def.Locked && existing.def.Owner != "" &&
			existing.def.Owner != def.Owner && !isAdmin {
			s.mu.RUnlock()
			return fmt.Errorf("function %q is locked by %q — only the owner or an admin can replace it",
				def.Name, existing.def.Owner)
		}
	}
	s.mu.RUnlock()

	// Check for circular dependency: the UDF must not reference itself
	deps, err := extractFuncDeps(def.Body)
	if err != nil {
		return fmt.Errorf("analyzing UDF %q body: %w", def.Name, err)
	}
	for _, dep := range deps {
		if dep == def.Name {
			return fmt.Errorf("UDF %q has circular dependency on itself", def.Name)
		}
	}

	// Check transitive circular dependencies
	s.mu.RLock()
	if err := s.checkCircular(def.Name, deps); err != nil {
		s.mu.RUnlock()
		return err
	}
	s.mu.RUnlock()

	// Compile the body
	body, err := compileUDFBody(def.Body, def.Params)
	if err != nil {
		return fmt.Errorf("compiling UDF %q: %w", def.Name, err)
	}

	// Register as a ScalarFunc in the default registry so it's callable
	// from normal SQL expressions via FuncCall
	s.mu.Lock()
	s.udfs[def.Name] = &compiledUDF{def: def, body: body, deps: deps}
	s.mu.Unlock()

	// Register a ScalarFunc wrapper that creates a UDFCall and evaluates it
	DefaultRegistry.Register(def.Name, s.makeScalarFunc(def.Name))

	return nil
}

// isBuiltinFunc checks if a function name is a builtin (registered during init).
// We maintain a static set of builtin names to prevent UDFs from shadowing them.
var builtinFuncNames map[string]bool

func init() {
	builtinFuncNames = make(map[string]bool)
	for _, name := range DefaultRegistry.Names() {
		builtinFuncNames[name] = true
	}
}

func isBuiltinFunc(name string) bool {
	return builtinFuncNames[strings.ToLower(name)]
}

// Unregister removes a UDF. Returns an error if the caller lacks permission.
// The caller parameter identifies who is attempting the drop; isAdmin bypasses
// ownership checks.
func (s *UDFStore) Unregister(name, caller string, isAdmin bool) error {
	name = strings.ToLower(name)
	s.mu.Lock()
	existing, ok := s.udfs[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("function %q does not exist", name)
	}
	if existing.def.Locked && existing.def.Owner != "" &&
		existing.def.Owner != caller && !isAdmin {
		s.mu.Unlock()
		return fmt.Errorf("function %q is locked by %q — only the owner or an admin can drop it",
			name, existing.def.Owner)
	}
	delete(s.udfs, name)
	s.mu.Unlock()
	DefaultRegistry.Unregister(name)
	return nil
}

// Get returns a UDF definition by name.
func (s *UDFStore) Get(name string) (UDFDef, bool) {
	s.mu.RLock()
	u, ok := s.udfs[strings.ToLower(name)]
	s.mu.RUnlock()
	if !ok {
		return UDFDef{}, false
	}
	return u.def, true
}

// List returns all registered UDF definitions.
func (s *UDFStore) List() []UDFDef {
	s.mu.RLock()
	defs := make([]UDFDef, 0, len(s.udfs))
	for _, u := range s.udfs {
		defs = append(defs, u.def)
	}
	s.mu.RUnlock()
	return defs
}

// CompileUDFCall creates a UDFCall expression node for calling a registered UDF
// with the given argument expressions. This is used by the expression compiler
// when it encounters a function call that matches a registered UDF.
func (s *UDFStore) CompileUDFCall(name string, argExprs []Expr) (Expr, error) {
	name = strings.ToLower(name)
	s.mu.RLock()
	u, ok := s.udfs[name]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown UDF: %q", name)
	}
	if len(argExprs) != len(u.def.Params) {
		return nil, fmt.Errorf("UDF %q expects %d arguments, got %d",
			name, len(u.def.Params), len(argExprs))
	}

	// Clone the body tree with fresh ParamRef args pointer
	argsSlice := make([]any, len(u.def.Params))
	argsPtr := &argsSlice
	body := cloneExprWithArgs(u.body, argsPtr)

	return &UDFCall{
		Name:     name,
		ArgExprs: argExprs,
		Body:     body,
		args:     argsSlice,
		argsPtr:  argsPtr,
	}, nil
}

// makeScalarFunc creates a ScalarFunc that evaluates the UDF body.
// This wraps the UDF so it can be called via the normal FuncCall path.
func (s *UDFStore) makeScalarFunc(name string) ScalarFunc {
	return func(args []any) any {
		s.mu.RLock()
		u, ok := s.udfs[name]
		s.mu.RUnlock()
		if !ok {
			return nil
		}

		// Create a temporary args slice and evaluate
		argsSlice := make([]any, len(args))
		copy(argsSlice, args)
		argsPtr := &argsSlice

		body := cloneExprWithArgs(u.body, argsPtr)
		return body.Eval(nil, 0)
	}
}

// checkCircular checks if registering a UDF with the given deps would create
// a circular dependency. Must be called with s.mu.RLock held.
func (s *UDFStore) checkCircular(name string, deps []string) error {
	visited := map[string]bool{name: true}
	stack := make([]string, len(deps))
	copy(stack, deps)

	for len(stack) > 0 {
		dep := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[dep] {
			return fmt.Errorf("circular dependency detected: %q depends on %q", name, dep)
		}
		visited[dep] = true
		if u, ok := s.udfs[dep]; ok {
			stack = append(stack, u.deps...)
		}
	}
	return nil
}

// compileUDFBody parses the body expression and replaces parameter references
// with ParamRef nodes. The returned Expr tree is a template — it must be
// cloned with fresh args pointers for each call site.
func compileUDFBody(body string, params []string) (Expr, error) {
	// Parse as: SELECT <body> FROM dual
	sql := "SELECT " + body + " FROM dual"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing body expression: %w", err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs) == 0 {
		return nil, fmt.Errorf("body must be a single expression")
	}
	aliased, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("body must be a single expression")
	}

	// Build param name → index map
	paramIdx := make(map[string]int, len(params))
	for i, p := range params {
		paramIdx[strings.ToLower(p)] = i
	}

	// Walk the AST replacing ColName nodes matching params with a marker,
	// then compile and substitute ParamRef nodes
	rewritten := rewriteParamRefs(aliased.Expr, paramIdx)
	return compileWithParamRefs(rewritten, paramIdx)
}

// rewriteParamRefs walks a vitess AST, but we compile directly with param awareness.
// Instead of rewriting the AST, we compile directly substituting params.
func compileWithParamRefs(node sqlparser.Expr, paramIdx map[string]int) (Expr, error) {
	if node == nil {
		return &Lit{Val: nil}, nil
	}
	switch n := node.(type) {
	case *sqlparser.ColName:
		name := strings.ToLower(n.Name.String())
		if idx, ok := paramIdx[name]; ok {
			return &ParamRef{Index: idx}, nil
		}
		// Not a parameter — treat as a regular column reference
		return &ColRef{Name: n.Name.String()}, nil

	case *sqlparser.SQLVal:
		return compileSQLVal(n)

	case *sqlparser.NullVal:
		return &Lit{Val: nil}, nil

	case sqlparser.BoolVal:
		return &Lit{Val: bool(n)}, nil

	case *sqlparser.BinaryExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		op := "+"
		switch n.Operator {
		case sqlparser.PlusStr:
			op = "+"
		case sqlparser.MinusStr:
			op = "-"
		case sqlparser.MultStr:
			op = "*"
		case sqlparser.DivStr:
			op = "/"
		case sqlparser.ModStr:
			op = "%"
		default:
			op = n.Operator
		}
		return &BinOp{Left: left, Right: right, Op: op}, nil

	case *sqlparser.UnaryExpr:
		operand, err := compileWithParamRefs(n.Expr, paramIdx)
		if err != nil {
			return nil, err
		}
		switch n.Operator {
		case sqlparser.UMinusStr:
			return &UnaryOp{Operand: operand, Op: "-"}, nil
		case sqlparser.UPlusStr:
			return &UnaryOp{Operand: operand, Op: "+"}, nil
		default:
			return operand, nil
		}

	case *sqlparser.ComparisonExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		// Handle IN/LIKE specially
		switch n.Operator {
		case sqlparser.InStr, sqlparser.NotInStr:
			tuple, ok := n.Right.(sqlparser.ValTuple)
			if !ok {
				return nil, fmt.Errorf("IN requires a value list")
			}
			var values []Expr
			for _, v := range tuple {
				compiled, err := compileWithParamRefs(v, paramIdx)
				if err != nil {
					return nil, err
				}
				values = append(values, compiled)
			}
			return &In{Expr: left, Values: values, Not: n.Operator == sqlparser.NotInStr}, nil
		case sqlparser.LikeStr, sqlparser.NotLikeStr:
			right, err := compileWithParamRefs(n.Right, paramIdx)
			if err != nil {
				return nil, err
			}
			return &Like{Expr: left, Pattern: right, Not: n.Operator == sqlparser.NotLikeStr}, nil
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		var op CmpOp
		switch n.Operator {
		case sqlparser.EqualStr:
			op = CmpEq
		case sqlparser.NotEqualStr:
			op = CmpNe
		case sqlparser.LessThanStr:
			op = CmpLt
		case sqlparser.LessEqualStr:
			op = CmpLe
		case sqlparser.GreaterThanStr:
			op = CmpGt
		case sqlparser.GreaterEqualStr:
			op = CmpGe
		default:
			op = CmpEq
		}
		return &Cmp{Left: left, Right: right, Op: op}, nil

	case *sqlparser.AndExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		return &And{Left: left, Right: right}, nil

	case *sqlparser.OrExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Or{Left: left, Right: right}, nil

	case *sqlparser.NotExpr:
		operand, err := compileWithParamRefs(n.Expr, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil

	case *sqlparser.ParenExpr:
		return compileWithParamRefs(n.Expr, paramIdx)

	case *sqlparser.FuncExpr:
		name := strings.ToLower(n.Name.String())
		var args []Expr
		for _, selExpr := range n.Exprs {
			switch e := selExpr.(type) {
			case *sqlparser.AliasedExpr:
				compiled, err := compileWithParamRefs(e.Expr, paramIdx)
				if err != nil {
					return nil, err
				}
				args = append(args, compiled)
			case *sqlparser.StarExpr:
				args = append(args, &Lit{Val: "*"})
			default:
				return nil, fmt.Errorf("unsupported function argument: %T", selExpr)
			}
		}
		if name == "coalesce" {
			return &Coalesce{Args: args}, nil
		}
		return &FuncCall{Name: name, Args: args}, nil

	case *sqlparser.CaseExpr:
		c := &Case{}
		if n.Expr != nil {
			var err error
			c.Operand, err = compileWithParamRefs(n.Expr, paramIdx)
			if err != nil {
				return nil, err
			}
		}
		for _, when := range n.Whens {
			cond, err := compileWithParamRefs(when.Cond, paramIdx)
			if err != nil {
				return nil, err
			}
			result, err := compileWithParamRefs(when.Val, paramIdx)
			if err != nil {
				return nil, err
			}
			c.Whens = append(c.Whens, CaseWhen{Cond: cond, Result: result})
		}
		if n.Else != nil {
			var err error
			c.Else, err = compileWithParamRefs(n.Else, paramIdx)
			if err != nil {
				return nil, err
			}
		}
		return c, nil

	case *sqlparser.IsExpr:
		operand, err := compileWithParamRefs(n.Expr, paramIdx)
		if err != nil {
			return nil, err
		}
		switch n.Operator {
		case sqlparser.IsNullStr:
			return &IsNull{Operand: operand, Not: false}, nil
		case sqlparser.IsNotNullStr:
			return &IsNull{Operand: operand, Not: true}, nil
		default:
			return nil, fmt.Errorf("unsupported IS operator in UDF: %s", n.Operator)
		}

	case *sqlparser.RangeCond:
		expr, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		low, err := compileWithParamRefs(n.From, paramIdx)
		if err != nil {
			return nil, err
		}
		hi, err := compileWithParamRefs(n.To, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Between{
			Expr: expr, Low: low, Hi: hi,
			Not: n.Operator == sqlparser.NotBetweenStr,
		}, nil

	case *sqlparser.ConvertExpr:
		operand, err := compileWithParamRefs(n.Expr, paramIdx)
		if err != nil {
			return nil, err
		}
		destType := "string"
		if n.Type != nil {
			destType = strings.ToLower(n.Type.Type)
		}
		return &Cast{Operand: operand, DestType: destType}, nil

	default:
		return &Lit{Val: sqlparser.String(node)}, nil
	}
}

// rewriteParamRefs is a no-op passthrough since we handle params in compileWithParamRefs.
func rewriteParamRefs(node sqlparser.Expr, _ map[string]int) sqlparser.Expr {
	return node
}

// extractFuncDeps extracts names of function calls from a SQL expression body.
func extractFuncDeps(body string) ([]string, error) {
	sql := "SELECT " + body + " FROM dual"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs) == 0 {
		return nil, nil
	}
	aliased, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, nil
	}
	var deps []string
	walkFuncDeps(aliased.Expr, &deps)
	return deps, nil
}

func walkFuncDeps(node sqlparser.Expr, deps *[]string) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *sqlparser.FuncExpr:
		name := strings.ToLower(n.Name.String())
		*deps = append(*deps, name)
		for _, selExpr := range n.Exprs {
			if ae, ok := selExpr.(*sqlparser.AliasedExpr); ok {
				walkFuncDeps(ae.Expr, deps)
			}
		}
	case *sqlparser.BinaryExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *sqlparser.UnaryExpr:
		walkFuncDeps(n.Expr, deps)
	case *sqlparser.ComparisonExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *sqlparser.AndExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *sqlparser.OrExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *sqlparser.NotExpr:
		walkFuncDeps(n.Expr, deps)
	case *sqlparser.ParenExpr:
		walkFuncDeps(n.Expr, deps)
	case *sqlparser.CaseExpr:
		if n.Expr != nil {
			walkFuncDeps(n.Expr, deps)
		}
		for _, w := range n.Whens {
			walkFuncDeps(w.Cond, deps)
			walkFuncDeps(w.Val, deps)
		}
		if n.Else != nil {
			walkFuncDeps(n.Else, deps)
		}
	case *sqlparser.IsExpr:
		walkFuncDeps(n.Expr, deps)
	case *sqlparser.RangeCond:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.From, deps)
		walkFuncDeps(n.To, deps)
	}
}

// cloneExprWithArgs deep-clones an expression tree, replacing all ParamRef
// nodes' args pointer with the given pointer. This allows each call site
// to have its own args buffer.
func cloneExprWithArgs(e Expr, argsPtr *[]any) Expr {
	if e == nil {
		return nil
	}
	switch n := e.(type) {
	case *ParamRef:
		return &ParamRef{Index: n.Index, args: argsPtr}
	case *ColRef:
		return &ColRef{Name: n.Name}
	case *Lit:
		return &Lit{Val: n.Val}
	case *BinOp:
		return &BinOp{
			Left:  cloneExprWithArgs(n.Left, argsPtr),
			Right: cloneExprWithArgs(n.Right, argsPtr),
			Op:    n.Op,
		}
	case *UnaryOp:
		return &UnaryOp{
			Operand: cloneExprWithArgs(n.Operand, argsPtr),
			Op:      n.Op,
		}
	case *Cmp:
		return &Cmp{
			Left:  cloneExprWithArgs(n.Left, argsPtr),
			Right: cloneExprWithArgs(n.Right, argsPtr),
			Op:    n.Op,
		}
	case *And:
		return &And{
			Left:  cloneExprWithArgs(n.Left, argsPtr),
			Right: cloneExprWithArgs(n.Right, argsPtr),
		}
	case *Or:
		return &Or{
			Left:  cloneExprWithArgs(n.Left, argsPtr),
			Right: cloneExprWithArgs(n.Right, argsPtr),
		}
	case *Not:
		return &Not{Operand: cloneExprWithArgs(n.Operand, argsPtr)}
	case *IsNull:
		return &IsNull{
			Operand: cloneExprWithArgs(n.Operand, argsPtr),
			Not:     n.Not,
		}
	case *FuncCall:
		args := make([]Expr, len(n.Args))
		for i, a := range n.Args {
			args[i] = cloneExprWithArgs(a, argsPtr)
		}
		return &FuncCall{Name: n.Name, Args: args}
	case *Coalesce:
		args := make([]Expr, len(n.Args))
		for i, a := range n.Args {
			args[i] = cloneExprWithArgs(a, argsPtr)
		}
		return &Coalesce{Args: args}
	case *Case:
		c := &Case{}
		if n.Operand != nil {
			c.Operand = cloneExprWithArgs(n.Operand, argsPtr)
		}
		c.Whens = make([]CaseWhen, len(n.Whens))
		for i, w := range n.Whens {
			c.Whens[i] = CaseWhen{
				Cond:   cloneExprWithArgs(w.Cond, argsPtr),
				Result: cloneExprWithArgs(w.Result, argsPtr),
			}
		}
		if n.Else != nil {
			c.Else = cloneExprWithArgs(n.Else, argsPtr)
		}
		return c
	case *In:
		values := make([]Expr, len(n.Values))
		for i, v := range n.Values {
			values[i] = cloneExprWithArgs(v, argsPtr)
		}
		return &In{
			Expr:   cloneExprWithArgs(n.Expr, argsPtr),
			Values: values,
			Not:    n.Not,
		}
	case *Between:
		return &Between{
			Expr: cloneExprWithArgs(n.Expr, argsPtr),
			Low:  cloneExprWithArgs(n.Low, argsPtr),
			Hi:   cloneExprWithArgs(n.Hi, argsPtr),
			Not:  n.Not,
		}
	case *Like:
		return &Like{
			Expr:    cloneExprWithArgs(n.Expr, argsPtr),
			Pattern: cloneExprWithArgs(n.Pattern, argsPtr),
			Not:     n.Not,
		}
	case *Cast:
		return &Cast{
			Operand:  cloneExprWithArgs(n.Operand, argsPtr),
			DestType: n.DestType,
		}
	default:
		// Unknown node type — return as-is (stateless nodes like Lit)
		return e
	}
}
