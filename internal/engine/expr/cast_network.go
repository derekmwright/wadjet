package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// castToNetwork is `CAST(x AS IPV4 / IPV6 / CIDR / MACADDR / UUID)`, which had
// no arm at all and reached `default: return v` — the operand handed back
// unchanged under a network DECLARATION, so
// `CAST('abc' AS IPV4)` answered "abc" where PostgreSQL raises 22P02, and a
// foreign parquet/CSV/JSON string column could not be converted to a native
// type on read at all (#1092, pinned until now as a divergence).
//
// The grammar is the type's ONE input grammar (parquet.NetworkTextValue), the
// same one the writer parses with and the same one a literal in a WHERE clause
// is classified by, so a value this cast produces is a value that column can
// hold. The RESULT is the type's own text — what a column of that type boxes
// as, and what the projection's vector stores. PORT and PROTOCOL are NOT here:
// they are int4-backed, their result is a number, and their domain is the
// int32 carrier #901 settled — the integer arm reads them, with the IANA name
// as the one addition (#986).
func castToNetwork(b *batch.RecordBatch, row int, operand Expr, v any, typ parquet.TypeID) any {
	// The column's DISPLAY text, not its raw encoding: an IPV4 or MAC ColRef
	// boxes as its int64 at some sites, and reading that as address text would
	// parse a decimal digit string (the #497 shape). castStringRender's
	// resolver is the one that undoes it.
	text := boxedTextOperand(b, row, operand, v)
	s, isText := stringOperand(text)
	if !isText {
		// A raw encoding that reached here unresolved still has one reading,
		// and it is not a refusal: render it as the type does.
		switch n := text.(type) {
		case int64:
			switch typ {
			case parquet.TypeIPv4:
				return batch.FormatIPv4(uint32(n))
			case parquet.TypeMAC:
				return batch.FormatMAC(uint64(n))
			}
		}
		// Anything else is a wrong TYPE PAIR, which PostgreSQL answers at
		// parse time with 42846 rather than a data exception; minting a data
		// exception for one here would be a different divergence.
		return v
	}
	val, st, ok := parquet.NetworkTextValue(typ, s)
	if !ok {
		return v
	}
	if st != parquet.NetTextOK {
		panic(fatalEval{parquet.NetworkTextError(typ, s, st)})
	}
	switch typ {
	case parquet.TypeIPv4:
		return batch.FormatIPv4(uint32(val.(int64)))
	case parquet.TypeMAC:
		return batch.FormatMAC(uint64(val.(int64)))
	case parquet.TypeIPv6:
		return batch.FormatIPv6(val.([]byte))
	case parquet.TypeUUID:
		return batch.FormatUUID(val.([]byte))
	case parquet.TypeCIDR:
		// The VALIDATED text, verbatim. A CIDR column stores its text
		// directly — it is the one network type whose storage is not the
		// value — so canonicalizing here would make the cast and the column
		// disagree about `'192.168/16'`. Recorded in ADR-0012.
		return val.(string)
	}
	return v
}

// networkCastType resolves a CAST destination SPELLING to the network type it
// names, for the spellings Cast.Eval's switch routes here. It is deliberately
// narrower than parquet.ParseTypeID: only the names this arm implements.
func networkCastType(dest string) (parquet.TypeID, bool) {
	switch dest {
	case "ipv4", "ip":
		return parquet.TypeIPv4, true
	case "ipv6":
		return parquet.TypeIPv6, true
	case "cidr":
		return parquet.TypeCIDR, true
	case "mac", "macaddr":
		return parquet.TypeMAC, true
	case "uuid", "guid":
		return parquet.TypeUUID, true
	}
	return 0, false
}
