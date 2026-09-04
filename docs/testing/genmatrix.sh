#!/bin/bash
# Generates PostgreSQL 17's UNION type-resolution verdict for every ordered
# pair of the 18 flat type-matrix columns. Output: one Go map line per pair.
set -u
URI="postgres://wadjet:wadjet@127.0.0.1:55432/wadjet_oracle?sslmode=disable"
psql "$URI" -q -c "DROP TABLE IF EXISTS e4m CASCADE" -c "
CREATE TABLE e4m(
  c_bool boolean, c_i32 integer, c_i64 bigint, c_f32 real, c_f64 double precision,
  c_str text, c_bytes bytea, c_ts timestamp, c_ipv4 inet, c_ipv6 inet,
  c_cidr cidr, c_mac macaddr, c_port integer, c_proto integer, c_dur bigint,
  c_uuid uuid, c_date date, c_dec numeric(18,4))" -c "
INSERT INTO e4m VALUES (true,1,1,1,1,'a','\\x01','2010-01-01','10.0.0.1','2001:db8::1',
  '10.0.0.0/8','00:11:22:33:44:55',80,6,1000,'00000000-0000-0000-0000-000000000001','2010-01-01',1.0)" > /dev/null

COLS="c_bool c_i32 c_i64 c_f32 c_f64 c_str c_bytes c_ts c_ipv4 c_ipv6 c_cidr c_mac c_port c_proto c_dur c_uuid c_date c_dec"
for a in $COLS; do
  for b in $COLS; do
    out=$(psql "$URI" -t -A -c "SELECT pg_typeof(v)::text FROM (SELECT $a AS v FROM e4m UNION ALL SELECT $b FROM e4m) q LIMIT 1" 2>&1)
    if echo "$out" | grep -q ERROR; then
      echo "\"$a|$b\": \"ERR\","
    else
      echo "\"$a|$b\": \"$(echo "$out" | tr -d '\n')\","
    fi
  done
done
psql "$URI" -q -c "DROP TABLE e4m" > /dev/null
