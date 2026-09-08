package exec

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// OHLCV — one MERGEABLE aggregate state that answers a whole bar.
//
//	ohlcv(ts, price, volume) -> ROW(open, high, low, close, volume, vwap)
//
// The pattern is ADR-0035's and this is its first instance: a state whose
// merge is ASSOCIATIVE and COMMUTATIVE is a state that can be computed
// per-task and combined, which is what lets one bar cross the stage DAG, the
// spill runs and the shuffle without the operator that finishes it ever seeing
// a raw row (ADR-0010's merge form, the shape varianceState and covarianceState
// already take).
//
//	n        rows folded in; 0 means the group is EMPTY and the bar is NULL
//	firstTS  the OPENING row's instant, epoch millis
//	firstPx  the price at that instant
//	lastTS   the CLOSING row's instant
//	lastPx   the price at that instant
//	high/low max / min price
//	sumVol   Σ volume
//	sumPV    Σ (price × volume)
//
// merge:
//
//	n      = a.n + b.n
//	first  = the (ts, px) LEXICOGRAPHIC MINIMUM of the two
//	last   = the (ts, px) LEXICOGRAPHIC MAXIMUM of the two
//	high   = max(high)          low = min(low)
//	sumVol = sum               sumPV = sum
//
// **The tiebreak is a VALUE.** Two rows sharing an instant have no order a
// query can see: a row POSITION is not observable across the arms — the single
// path reads one file, the DAG reads four in whatever order tasks finish — so
// picking "the first one that arrived" would make the bar depend on the plan.
// `open` is therefore the price of the row with the smallest (ts, price) and
// `close` the price of the row with the largest, which is exactly PostgreSQL's
//
//	(array_agg(px ORDER BY ts, px))[1]        -- open
//	(array_agg(px ORDER BY ts DESC, px DESC))[1]  -- close
//
// and that spelling is the value oracle for every cell of the gate.
//
// **NULL rule.** A row is skipped when ANY of ts, price, volume is NULL —
// PostgreSQL's rule for a multi-argument aggregate, measured on 17.11:
// regr_count(y,x) over (1,1),(2,NULL),(NULL,3),(4,4) is 2, not 4.
//
// **Domain.** Decided ONCE per aggregate from the input columns' declared
// types, never per row. It is EXACT — Int128 at a fixed scale, so the sums
// carry every digit — unless price or volume is approximate, in which case the
// whole state is float64, because one float operand makes the quotient float
// on the server too. An exact sum that leaves the 128-bit carrier is 22003,
// never a wrapped or narrowed number (ADR-0024).
type ohlcvState struct {
	dom ohlcvDomain
	// fields is the DECLARED ROW this state finishes as. It is written by the
	// operator that computed the values, and it travels INSIDE the encoding,
	// because the operator is the only layer that always knows it: the
	// planner cannot walk a COMPUTED argument to a catalog column, and the
	// coordinator's fold sees a string and no vectors at all. Carrying it
	// here is what makes `ohlcv(ts, px*2, vol)` answer the same on the DAG as
	// in process instead of failing at the fold (#965).
	fields []parquet.Column

	n       int64
	firstTS int64
	lastTS  int64

	// Exact carriers (dom.exact). Prices are at dom.priceScale, sumVol at
	// dom.volScale, sumPV at dom.pvScale.
	firstPx, lastPx, high, low batch.Int128
	sumVol, sumPV              batch.Int128

	// Approximate carriers (!dom.exact).
	firstPxF, lastPxF, highF, lowF float64
	sumVolF, sumPVF                float64

	// overflow latches an exact sum that left the carrier. The bar is then a
	// refusal, not a number: FinalizeOhlcvState reports it and the caller
	// raises 22003.
	overflow bool
}

// ohlcvDomain is everything about a bar that is decided at PLAN time and holds
// for every row and every merge: which carrier the numbers use, and at what
// scales. It travels INSIDE the encoded state so the coordinator's fold needs
// nothing but the string.
type ohlcvDomain struct {
	exact      bool
	priceScale int // scale of open/high/low/close
	volScale   int // scale of sumVol
	pvScale    int // scale of sumPV = priceScale + volScale
}

// --- domain and declared output ---------------------------------------------

// OhlcvPriceInput reports whether a column type can be a bar's PRICE or its
// VOLUME, and gives the (exact, scale) pair the state carries it as.
//
// The accepted set is the numeric one. A bar over a STRING or an IPv4 is not a
// narrower bar, it is a different question, and PostgreSQL refuses a wrongly
// typed aggregate argument rather than guessing (42883).
func OhlcvPriceInput(t parquet.TypeID, scale int) (exact bool, outScale int, ok bool) {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		return true, 0, true
	case parquet.TypeDecimal:
		return true, scale, true
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return false, 0, true
	}
	return false, 0, false
}

// OhlcvTimeInput reports whether a column type can be a bar's ORDERING key.
// TIMESTAMP and DATE, which are the two this engine can read as an instant
// without guessing a unit — the same pair `time_bucket` reads.
func OhlcvTimeInput(t parquet.TypeID) bool {
	return t == parquet.TypeTimestamp || t == parquet.TypeDate
}

// OhlcvDomainFor resolves the carrier from the three input declarations. It is
// THE one place the question is answered: the plan-time declaration
// (physical.aggSpecOutputType), the operator's own output schema and the
// coordinator's fold all come through it, so a bar cannot be declared one way
// and computed another.
func OhlcvDomainFor(priceType parquet.TypeID, priceScale int,
	volType parquet.TypeID, volScale int) (ohlcvDomain, bool) {
	pExact, pScale, ok := OhlcvPriceInput(priceType, priceScale)
	if !ok {
		return ohlcvDomain{}, false
	}
	vExact, vScale, ok := OhlcvPriceInput(volType, volScale)
	if !ok {
		return ohlcvDomain{}, false
	}
	d := ohlcvDomain{exact: pExact && vExact, priceScale: pScale, volScale: vScale}
	if !d.exact {
		d.priceScale, d.volScale = 0, 0
	}
	d.pvScale = d.priceScale + d.volScale
	if d.pvScale > batch.MaxDecimalScale {
		// Two wide-scale inputs whose product has no exact carrier. Refusing
		// the PLAN is the honest answer: the alternative is a vwap silently
		// rounded to a scale nobody asked for.
		return ohlcvDomain{}, false
	}
	return d, true
}

// OhlcvFieldNames is the bar's shape. Order is fixed and is the order the
// wire renders a composite in — `(open,high,low,close,volume,vwap)`.
var OhlcvFieldNames = []string{"open", "high", "low", "close", "volume", "vwap"}

// OhlcvOutputFields is the bar's declared ROW, derived from the input columns.
// Each field declares exactly what its own spelled-out aggregate declares, so
// a bar and the five scalar aggregates beside it never disagree:
//
//	open/high/low/close   the PRICE column's own type — four values the column
//	                      HELD, which is MIN_BY/MAX_BY's rule and PostgreSQL's
//	                      (min(int4) is integer, min(real) is real,
//	                      min(numeric(9,2)) keeps its (p,s))
//	volume                SUM(volume)'s type — exec.IntegerAccOutputType, the
//	                      ONE integer accumulator table
//	vwap                  AVG(price)'s type — a vwap IS a weighted average of
//	                      price, so it takes the average's scale rule
//
// ok=false for an input type that cannot be a bar, which the planner turns
// into the 42883 refusal.
func OhlcvOutputFields(price parquet.Column, vol parquet.Column) ([]parquet.Column, bool) {
	dom, ok := OhlcvDomainFor(price.Type, price.Scale, vol.Type, vol.Scale)
	if !ok {
		return nil, false
	}
	pxCol := func(name string) parquet.Column {
		c := parquet.Column{Name: name, Type: price.Type, Nullable: true}
		if price.Type == parquet.TypeDecimal {
			c.Precision, c.Scale = price.Precision, price.Scale
		}
		return c
	}
	volOut := parquet.Column{Name: "volume", Type: vol.Type, Nullable: true}
	if t, p, s, ok := IntegerAccOutputType(false, vol.Type); ok {
		volOut.Type, volOut.Precision, volOut.Scale = t, p, s
	} else if vol.Type == parquet.TypeDecimal {
		volOut.Type = parquet.TypeDecimal
		volOut.Precision, volOut.Scale = batch.MaxDecimalPrecision, vol.Scale
	} else {
		volOut.Type = parquet.TypeFloat64
	}
	vwap := parquet.Column{Name: "vwap", Type: parquet.TypeFloat64, Nullable: true}
	if dom.exact {
		vwap.Type = parquet.TypeDecimal
		vwap.Precision, vwap.Scale = batch.MaxDecimalPrecision, batch.AvgScale(dom.priceScale)
	}
	return []parquet.Column{
		pxCol("open"), pxCol("high"), pxCol("low"), pxCol("close"), volOut, vwap,
	}, true
}

// --- observation and merge ---------------------------------------------------

// ohlcvReader reads one row's three values out of the input vectors. Resolved
// ONCE per batch from the vectors' own types, which is what keeps the state's
// domain and the columns it reads the same decision.
type ohlcvReader struct {
	dom     ohlcvDomain
	tsVec   *batch.Vector
	pxVec   *batch.Vector
	volVec  *batch.Vector
	resolve bool
}

// ohlcvInstantMillis reads a TIMESTAMP or DATE cell as epoch milliseconds —
// the unit `time_bucket` and every other instant consumer here use. A DATE is
// its UTC midnight, which is the instant PostgreSQL's date->timestamp cast
// gives.
func ohlcvInstantMillis(v *batch.Vector, row int) (int64, bool) {
	switch v.Type {
	case parquet.TypeTimestamp:
		if row < len(v.Int64Data) {
			return v.Int64Data[row], true
		}
	case parquet.TypeDate:
		if row < len(v.Int32Data) {
			return int64(v.Int32Data[row]) * 86_400_000, true
		}
	}
	return 0, false
}

// ohlcvExactCell reads a numeric cell as an Int128 at its own scale.
func ohlcvExactCell(v *batch.Vector, row int) (batch.Int128, bool) {
	switch v.Type {
	case parquet.TypeInt64:
		if row < len(v.Int64Data) {
			return batch.Int128From(v.Int64Data[row]), true
		}
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		if row < len(v.Int32Data) {
			return batch.Int128From(int64(v.Int32Data[row])), true
		}
	case parquet.TypeDecimal:
		if row < len(v.DecimalData.Data) {
			return v.DecimalData.Data[row], true
		}
	}
	return batch.Int128{}, false
}

func ohlcvFloatCell(v *batch.Vector, row int) (float64, bool) {
	switch v.Type {
	case parquet.TypeFloat64:
		if row < len(v.Float64Data) {
			return v.Float64Data[row], true
		}
	case parquet.TypeFloat32:
		if row < len(v.Float32Data) {
			return float64(v.Float32Data[row]), true
		}
	case parquet.TypeInt64:
		if row < len(v.Int64Data) {
			return float64(v.Int64Data[row]), true
		}
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		if row < len(v.Int32Data) {
			return float64(v.Int32Data[row]), true
		}
	case parquet.TypeDecimal:
		if row < len(v.DecimalData.Data) {
			return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale), true
		}
	}
	return 0, false
}

// observe folds one row in. The caller has already established that none of
// the three columns is NULL at this row — that check is PostgreSQL's
// multi-argument rule and it belongs beside the other aggregates' null checks,
// not here.
func (s *ohlcvState) observe(r ohlcvReader, row int) {
	ts, ok := ohlcvInstantMillis(r.tsVec, row)
	if !ok {
		return
	}
	if s.dom.exact {
		px, ok1 := ohlcvExactCell(r.pxVec, row)
		vol, ok2 := ohlcvExactCell(r.volVec, row)
		if !ok1 || !ok2 {
			return
		}
		s.observeExact(ts, px, vol)
		return
	}
	px, ok1 := ohlcvFloatCell(r.pxVec, row)
	vol, ok2 := ohlcvFloatCell(r.volVec, row)
	if !ok1 || !ok2 {
		return
	}
	s.observeFloat(ts, px, vol)
}

func (s *ohlcvState) observeExact(ts int64, px, vol batch.Int128) {
	if s.n == 0 {
		s.firstTS, s.firstPx = ts, px
		s.lastTS, s.lastPx = ts, px
		s.high, s.low = px, px
		s.sumVol = batch.Int128{}
		s.sumPV = batch.Int128{}
	} else {
		if ohlcvBefore(ts, px, s.firstTS, s.firstPx) {
			s.firstTS, s.firstPx = ts, px
		}
		if ohlcvBefore(s.lastTS, s.lastPx, ts, px) {
			s.lastTS, s.lastPx = ts, px
		}
		if px.Cmp(s.high) > 0 {
			s.high = px
		}
		if px.Cmp(s.low) < 0 {
			s.low = px
		}
	}
	s.n++
	if v, st := batch.DecimalAdd(s.sumVol, s.dom.volScale, vol, s.dom.volScale, s.dom.volScale); st == batch.DecimalOK {
		s.sumVol = v
	} else {
		s.overflow = true
	}
	// price × volume at the product scale: exact, no rounding, because the
	// output scale IS the sum of the input scales.
	if pv, st := batch.DecimalMul(px, s.dom.priceScale, vol, s.dom.volScale, s.dom.pvScale); st == batch.DecimalOK {
		if v, st2 := batch.DecimalAdd(s.sumPV, s.dom.pvScale, pv, s.dom.pvScale, s.dom.pvScale); st2 == batch.DecimalOK {
			s.sumPV = v
		} else {
			s.overflow = true
		}
	} else {
		s.overflow = true
	}
}

func (s *ohlcvState) observeFloat(ts int64, px, vol float64) {
	if s.n == 0 {
		s.firstTS, s.firstPxF = ts, px
		s.lastTS, s.lastPxF = ts, px
		s.highF, s.lowF = px, px
		s.sumVolF, s.sumPVF = 0, 0
	} else {
		if ohlcvBeforeF(ts, px, s.firstTS, s.firstPxF) {
			s.firstTS, s.firstPxF = ts, px
		}
		if ohlcvBeforeF(s.lastTS, s.lastPxF, ts, px) {
			s.lastTS, s.lastPxF = ts, px
		}
		if kernel.CompareFloat64(px, s.highF) > 0 {
			s.highF = px
		}
		if kernel.CompareFloat64(px, s.lowF) < 0 {
			s.lowF = px
		}
	}
	s.n++
	s.sumVolF += vol
	s.sumPVF += px * vol
}

// ohlcvBefore is the (ts, price) lexicographic order that decides open and
// close. It is a TOTAL order on values, which is what makes the merge
// commutative and every arm agree on a tie.
func ohlcvBefore(aTS int64, aPx batch.Int128, bTS int64, bPx batch.Int128) bool {
	if aTS != bTS {
		return aTS < bTS
	}
	return aPx.Cmp(bPx) < 0
}

// ohlcvBeforeF is the same order over the approximate carrier. It compares
// through kernel.CompareFloat64, which is the order this engine gives REAL and
// DOUBLE PRECISION everywhere else — PostgreSQL's, where NaN sorts ABOVE every
// number — so a NaN price cannot silently become the bar's low or its open.
func ohlcvBeforeF(aTS int64, aPx float64, bTS int64, bPx float64) bool {
	if aTS != bTS {
		return aTS < bTS
	}
	return kernel.CompareFloat64(aPx, bPx) < 0
}

// merge folds another partial's bar into this one. Same law on every path: a
// parallel clone's state, a spilled run's, a worker's partial crossing the
// DAG.
func (s *ohlcvState) merge(o *ohlcvState) {
	if o == nil || o.n == 0 {
		return
	}
	if s.n == 0 {
		// The DESTINATION's domain is kept, always. Within one aggregate the
		// carrier is ONE decision (OhlcvDomainFor, taken from the input
		// declarations), so a merge is never the place to change it; a caller
		// folding into a state it has not typed yet sets `dom` from the
		// source FIRST and explicitly — MergeOhlcvStates, mergeExtraState and
		// the AggOhlcvStateMerge arm each do. Reading it out of the source
		// here would let a float-domain accumulator silently become an exact
		// one because the first partial it saw was.
		dom, fields := s.dom, s.fields
		*s = *o
		s.dom = dom
		if len(fields) == len(OhlcvFieldNames) {
			s.fields = fields
		}
		return
	}
	s.overflow = s.overflow || o.overflow
	if s.dom.exact {
		if ohlcvBefore(o.firstTS, o.firstPx, s.firstTS, s.firstPx) {
			s.firstTS, s.firstPx = o.firstTS, o.firstPx
		}
		if ohlcvBefore(s.lastTS, s.lastPx, o.lastTS, o.lastPx) {
			s.lastTS, s.lastPx = o.lastTS, o.lastPx
		}
		if o.high.Cmp(s.high) > 0 {
			s.high = o.high
		}
		if o.low.Cmp(s.low) < 0 {
			s.low = o.low
		}
		if v, st := batch.DecimalAdd(s.sumVol, s.dom.volScale, o.sumVol, s.dom.volScale, s.dom.volScale); st == batch.DecimalOK {
			s.sumVol = v
		} else {
			s.overflow = true
		}
		if v, st := batch.DecimalAdd(s.sumPV, s.dom.pvScale, o.sumPV, s.dom.pvScale, s.dom.pvScale); st == batch.DecimalOK {
			s.sumPV = v
		} else {
			s.overflow = true
		}
	} else {
		if ohlcvBeforeF(o.firstTS, o.firstPxF, s.firstTS, s.firstPxF) {
			s.firstTS, s.firstPxF = o.firstTS, o.firstPxF
		}
		if ohlcvBeforeF(s.lastTS, s.lastPxF, o.lastTS, o.lastPxF) {
			s.lastTS, s.lastPxF = o.lastTS, o.lastPxF
		}
		if kernel.CompareFloat64(o.highF, s.highF) > 0 {
			s.highF = o.highF
		}
		if kernel.CompareFloat64(o.lowF, s.lowF) < 0 {
			s.lowF = o.lowF
		}
		s.sumVolF += o.sumVolF
		s.sumPVF += o.sumPVF
	}
	s.n += o.n
}

func (s *ohlcvState) memBytes() int64 {
	// One fixed-size struct per group, no retained boxes.
	return 160
}

// --- the finished bar --------------------------------------------------------

// value renders the merged state as the ROW box the output vector takes, or
// nil for an empty group — PostgreSQL's "an aggregate over no rows is NULL".
//
// fields is the declared ROW, which decides how each number is narrowed: the
// state carries every digit and the declaration says how many the column
// holds. A number that does not fit is 22003, never a narrower one (ADR-0024).
func (s *ohlcvState) value(fields []parquet.Column) (any, error) {
	if s.n == 0 {
		return nil, nil
	}
	if s.overflow {
		return nil, ohlcvOverflow()
	}
	if len(fields) != len(OhlcvFieldNames) {
		return nil, sqlerr.New("XX000",
			"ohlcv: the bar has no declared field list (%d of %d), so there is no "+
				"ROW to write it into", len(fields), len(OhlcvFieldNames))
	}
	out := make(map[string]any, len(fields))
	if s.dom.exact {
		for i, v := range []batch.Int128{s.firstPx, s.high, s.low, s.lastPx} {
			cell, err := ohlcvNarrowExact(v, s.dom.priceScale, fields[i])
			if err != nil {
				return nil, err
			}
			out[fields[i].Name] = cell
		}
		vol, err := ohlcvNarrowExact(s.sumVol, s.dom.volScale, fields[4])
		if err != nil {
			return nil, err
		}
		out["volume"] = vol
		vwap, err := s.exactVwap(fields[5])
		if err != nil {
			return nil, err
		}
		out["vwap"] = vwap
		return out, nil
	}
	for i, v := range []float64{s.firstPxF, s.highF, s.lowF, s.lastPxF} {
		out[fields[i].Name] = ohlcvNarrowFloat(v, fields[i])
	}
	out["volume"] = ohlcvNarrowFloat(s.sumVolF, fields[4])
	if s.sumVolF == 0 {
		// Division by a zero total. PostgreSQL's float division by zero is
		// 22012, and a bar whose volume is zero has no weighted average, so
		// the field is NULL rather than an infinity nothing can compare.
		out["vwap"] = nil
	} else {
		out["vwap"] = s.sumPVF / s.sumVolF
	}
	return out, nil
}

// exactVwap is Σ(price×volume) / Σ(volume) through the engine's OWN decimal
// division at the declared scale — batch.DecimalDivAt, half away from zero,
// ADR-0024's rounding. Reusing it is what makes `(bar).vwap` equal the same
// query's `SUM(price*volume)/SUM(volume)` digit for digit instead of
// approximately.
func (s *ohlcvState) exactVwap(field parquet.Column) (any, error) {
	if s.sumVol.IsZero() {
		return nil, nil
	}
	q, st := batch.DecimalDivAt(s.sumPV, s.dom.pvScale, s.sumVol, s.dom.volScale,
		field.Precision, field.Scale)
	if st != batch.DecimalOK {
		return nil, ohlcvOverflow()
	}
	return q.FormatDecimal(field.Scale), nil
}

// ohlcvNarrowExact renders one exact carrier as the declared field's own Go
// box, the same box a column of that type produces.
func ohlcvNarrowExact(v batch.Int128, scale int, field parquet.Column) (any, error) {
	switch field.Type {
	case parquet.TypeDecimal:
		r, ok := batch.Rescale(v, scale, field.Scale)
		if !ok || !batch.DecimalFitsPrecision(r, field.Precision) {
			return nil, ohlcvOverflow()
		}
		return r.FormatDecimal(field.Scale), nil
	case parquet.TypeInt64:
		r, ok := batch.Rescale(v, scale, 0)
		if !ok || !r.FitsInt64() {
			return nil, ohlcvOverflow()
		}
		return r.ToInt64(), nil
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		r, ok := batch.Rescale(v, scale, 0)
		if !ok || !r.FitsInt64() {
			return nil, ohlcvOverflow()
		}
		n := r.ToInt64()
		if n > math.MaxInt32 || n < math.MinInt32 {
			return nil, ohlcvOverflow()
		}
		return int32(n), nil
	case parquet.TypeFloat64:
		return v.ToFloat64(scale), nil
	case parquet.TypeFloat32:
		return float32(v.ToFloat64(scale)), nil
	}
	return nil, sqlerr.New("XX000", "ohlcv: no carrier for declared field %s %v",
		field.Name, field.Type)
}

func ohlcvNarrowFloat(v float64, field parquet.Column) any {
	if field.Type == parquet.TypeFloat32 {
		return float32(v)
	}
	return v
}

// ohlcvOverflow is ADR-0024's answer for an exact number with no carrier: the
// SQLSTATE PostgreSQL raises for numeric overflow, never a wrapped or
// silently rescaled value.
func ohlcvOverflow() error {
	return sqlerr.New("22003", "ohlcv: the bar does not fit its declared type")
}

// --- the encoded partial state ----------------------------------------------

// ohlcvStateWidth is the encoded width of a bar's partial state: 136 raw bytes
// hex-encoded to 272 ASCII characters.
//
//	[0]       format version (1)
//	[1]       flags: bit0 exact, bit1 overflow
//	[2]       carrier price scale  [3] volume scale  [4] product scale
//	[5:8]     the PRICE field's declared (type, precision, scale)
//	[8:11]    the VOLUME field's declared (type, precision, scale)
//	[11:14]   the VWAP field's declared (type, precision, scale)
//	[14:16]   reserved, zero
//	[16:24]   n            int64 BE
//	[24:32]   firstTS      int64 BE
//	[32:40]   lastTS       int64 BE
//	[40:56]   open   [56:72] high   [72:88] low   [88:104] close
//	[104:120] sumVolume    [120:136] sumPriceTimesVolume
//
// A value slot is an Int128 (Hi then Lo, big-endian) when exact, and a float64
// in its low 8 bytes when not. The state travels as a STRING column — through
// parquet, the .wshf shuffle format and the NATS gather — for the reason
// varianceState.encode records: every one of those is happier with text than
// with arbitrary bytes, and float64 bits round-trip exactly through hex.
//
// Both the carrier AND the declared ROW are in the header, so the
// coordinator's fold finishes a bar with nothing but the string. Everything
// there fits a byte: 22 type ids, and a DECIMAL's precision and scale top out
// at 38.
const ohlcvStateWidth = 272

const ohlcvStateBytes = 136

func (s *ohlcvState) encode() string {
	var buf [ohlcvStateBytes]byte
	buf[0] = 1
	if s.dom.exact {
		buf[1] |= 1
	}
	if s.overflow {
		buf[1] |= 2
	}
	buf[2] = byte(s.dom.priceScale)
	buf[3] = byte(s.dom.volScale)
	buf[4] = byte(s.dom.pvScale)
	// The declared ROW: the price group (all four fields share it), the
	// volume field and the vwap field.
	putOhlcvField(buf[5:8], s.fields, 0)
	putOhlcvField(buf[8:11], s.fields, 4)
	putOhlcvField(buf[11:14], s.fields, 5)
	binary.BigEndian.PutUint64(buf[16:24], uint64(s.n))
	binary.BigEndian.PutUint64(buf[24:32], uint64(s.firstTS))
	binary.BigEndian.PutUint64(buf[32:40], uint64(s.lastTS))
	if s.dom.exact {
		putOhlcvInt128(buf[40:56], s.firstPx)
		putOhlcvInt128(buf[56:72], s.high)
		putOhlcvInt128(buf[72:88], s.low)
		putOhlcvInt128(buf[88:104], s.lastPx)
		putOhlcvInt128(buf[104:120], s.sumVol)
		putOhlcvInt128(buf[120:136], s.sumPV)
	} else {
		putOhlcvFloat(buf[40:56], s.firstPxF)
		putOhlcvFloat(buf[56:72], s.highF)
		putOhlcvFloat(buf[72:88], s.lowF)
		putOhlcvFloat(buf[88:104], s.lastPxF)
		putOhlcvFloat(buf[104:120], s.sumVolF)
		putOhlcvFloat(buf[120:136], s.sumPVF)
	}
	return hex.EncodeToString(buf[:])
}

// putOhlcvField writes one declared field's (type, precision, scale). An
// absent declaration writes zeroes, which decode reads back as "not declared"
// and the caller then supplies — the shape a state built before its operator
// resolved its columns has.
func putOhlcvField(dst []byte, fields []parquet.Column, i int) {
	if i >= len(fields) {
		return
	}
	dst[0] = byte(fields[i].Type)
	dst[1] = byte(fields[i].Precision)
	dst[2] = byte(fields[i].Scale)
}

// getOhlcvFields rebuilds the declared ROW from the header. ok=false when the
// header carries no declaration at all.
func getOhlcvFields(buf []byte) ([]parquet.Column, bool) {
	if buf[5] == 0 && buf[8] == 0 && buf[11] == 0 {
		return nil, false
	}
	col := func(name string, at int) parquet.Column {
		return parquet.Column{
			Name: name, Type: parquet.TypeID(buf[at]), Nullable: true,
			Precision: int(buf[at+1]), Scale: int(buf[at+2]),
		}
	}
	out := make([]parquet.Column, 0, len(OhlcvFieldNames))
	for _, n := range OhlcvFieldNames[:4] {
		out = append(out, col(n, 5))
	}
	out = append(out, col("volume", 8), col("vwap", 11))
	return out, true
}

func putOhlcvInt128(dst []byte, v batch.Int128) {
	binary.BigEndian.PutUint64(dst[0:8], uint64(v.Hi))
	binary.BigEndian.PutUint64(dst[8:16], v.Lo)
}

func getOhlcvInt128(src []byte) batch.Int128 {
	return batch.Int128{
		Hi: int64(binary.BigEndian.Uint64(src[0:8])),
		Lo: binary.BigEndian.Uint64(src[8:16]),
	}
}

func putOhlcvFloat(dst []byte, v float64) {
	binary.BigEndian.PutUint64(dst[0:8], math.Float64bits(v))
}

func getOhlcvFloat(src []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(src[0:8]))
}

// decodeOhlcvState parses encode's output. ok=false for anything else, which
// the caller treats as "no rows to merge" rather than as a valid empty
// partial — varianceState's rule, for the same reason.
func decodeOhlcvState(s string) (ohlcvState, bool) {
	if len(s) != ohlcvStateWidth {
		return ohlcvState{}, false
	}
	var buf [ohlcvStateBytes]byte
	if _, err := hex.Decode(buf[:], []byte(s)); err != nil {
		return ohlcvState{}, false
	}
	if buf[0] != 1 {
		return ohlcvState{}, false
	}
	st := ohlcvState{
		dom: ohlcvDomain{
			exact:      buf[1]&1 != 0,
			priceScale: int(buf[2]),
			volScale:   int(buf[3]),
			pvScale:    int(buf[4]),
		},
		overflow: buf[1]&2 != 0,
		n:        int64(binary.BigEndian.Uint64(buf[16:24])),
		firstTS:  int64(binary.BigEndian.Uint64(buf[24:32])),
		lastTS:   int64(binary.BigEndian.Uint64(buf[32:40])),
	}
	if f, ok := getOhlcvFields(buf[:]); ok {
		st.fields = f
	}
	if st.dom.exact {
		st.firstPx = getOhlcvInt128(buf[40:56])
		st.high = getOhlcvInt128(buf[56:72])
		st.low = getOhlcvInt128(buf[72:88])
		st.lastPx = getOhlcvInt128(buf[88:104])
		st.sumVol = getOhlcvInt128(buf[104:120])
		st.sumPV = getOhlcvInt128(buf[120:136])
	} else {
		st.firstPxF = getOhlcvFloat(buf[40:56])
		st.highF = getOhlcvFloat(buf[56:72])
		st.lowF = getOhlcvFloat(buf[72:88])
		st.lastPxF = getOhlcvFloat(buf[88:104])
		st.sumVolF = getOhlcvFloat(buf[104:120])
		st.sumPVF = getOhlcvFloat(buf[120:136])
	}
	return st, true
}

// MergeOhlcvStates folds one encoded partial into another and re-emits the
// encoded result. It is what a MERGE stage runs, and it is the same law the
// in-process merge runs — one function, so a fan-in tree of merge stages
// cannot answer differently from a single one.
//
// An unparseable input is skipped rather than treated as an empty bar.
func MergeOhlcvStates(acc, next string) string {
	a, aok := decodeOhlcvState(acc)
	b, bok := decodeOhlcvState(next)
	switch {
	case !aok && !bok:
		return ""
	case !aok:
		return next
	case !bok:
		return acc
	}
	if a.n == 0 {
		a.dom = b.dom
	}
	a.merge(&b)
	return a.encode()
}

// FinalizeOhlcvState decodes a fully merged partial and finishes it as the bar
// its own header describes. It is the coordinator/worker fold's entry point
// (worker/var_fold.go's family), and it goes through exactly the same
// ohlcvState.value the single-process operator calls.
//
// It takes NO declaration argument, and that is the point: the operator that
// computed the values wrote their declared types beside them, so the fold
// cannot finish a bar against a field list the values were not computed for.
// The planner's list is a DECLARATION for the stage and the wire; this is the
// one the value belongs to.
//
// ok=false with a nil error means SQL NULL — an empty group, or a state
// nothing wrote.
func FinalizeOhlcvState(encoded string) (any, []parquet.Column, bool, error) {
	st, ok := decodeOhlcvState(encoded)
	if !ok {
		return nil, nil, false, nil
	}
	if st.n == 0 {
		// An EMPTY bar is NULL whatever shape it would have had, and it is
		// the one state that legitimately carries no declaration: a partial
		// task whose filter matched no rows never resolved its input columns,
		// so it had nothing to declare from. That identity row is the shape
		// #685 is about, one type over.
		return nil, st.fields, false, nil
	}
	fields := st.fields
	if len(fields) != len(OhlcvFieldNames) {
		return nil, nil, false, sqlerr.New("XX000",
			"ohlcv: the encoded state carries no declared fields")
	}
	v, err := st.value(fields)
	if err != nil {
		return nil, nil, false, err
	}
	if v == nil {
		return nil, fields, false, nil
	}
	return v, fields, true, nil
}

// OhlcvStateColumnPrefix marks the synthetic column a partial bar travels in.
// The `#` is illegal in an identifier, so it cannot collide with a user's
// column — the convention __var_state#<kind>#<out> established.
const OhlcvStateColumnPrefix = "__ohlcv_state#"

// OhlcvStateColumn is the synthetic name a partial bar for output column `out`
// travels under.
func OhlcvStateColumn(out string) string {
	return OhlcvStateColumnPrefix + out
}

// OhlcvStateOutput reads back the output column an OhlcvStateColumn names.
func OhlcvStateOutput(col string) (string, bool) {
	if !strings.HasPrefix(col, OhlcvStateColumnPrefix) {
		return "", false
	}
	return col[len(OhlcvStateColumnPrefix):], true
}

// ohlcvFields is the declared ROW for aggregate j — the planner's list when it
// could resolve the input columns, and otherwise the same list re-derived from
// the vectors the operator actually reads.
//
// It is ONE list either way, from exec.OhlcvOutputFields. A hand-rolled second
// derivation here is how a bar comes to be declared one thing and written
// another; the runtime fallback exists because a computed argument, or a
// partial's own output read by a merge stage, gives the planner no column to
// walk (aggSpecOutputType's "unresolved" answer).
// declaredFields is the ROW this state finishes as: its OWN declaration when
// it has one (stamped at construction, or carried in from a decoded partial),
// and the operator's derivation otherwise. The state's own comes first because
// it is the one the VALUES were computed against.
func (s *ohlcvState) declaredFields(h *HashAggregate, j int) []parquet.Column {
	if len(s.fields) == len(OhlcvFieldNames) {
		return s.fields
	}
	return h.ohlcvFields(j)
}

func (h *HashAggregate) ohlcvFields(j int) []parquet.Column {
	if j < len(h.Aggs) && len(h.Aggs[j].OutputFields) == len(OhlcvFieldNames) {
		return h.Aggs[j].OutputFields
	}
	px, vol := -1, -1
	if j < len(h.aggColIdx) {
		px = h.aggColIdx[j]
	}
	if j < len(h.aggColIdx3) {
		vol = h.aggColIdx3[j]
	}
	if px >= 0 && vol >= 0 && j < len(h.aggInputMeta) {
		priceCol := h.aggInputMeta[j]
		volCol := parquet.Column{Name: "volume"}
		if j < len(h.aggVolMeta) {
			volCol = h.aggVolMeta[j]
		}
		if fields, ok := OhlcvOutputFields(priceCol, volCol); ok {
			return fields
		}
	}
	// Nothing resolved: NO declaration. A guessed one is the worst answer
	// available — it builds a ROW vector whose children are the wrong types
	// and writes the values into them — so value() reports the absence
	// instead, and every caller that can supply one (a decoded partial's own
	// header) is asked first.
	return nil
}

// The wire spellings of the bar's partial and merge forms. They are function
// NAMES on an AggSpec, the way "var_state" and "covar_state" are, so a stage
// can ask for a partial bar without a second enum crossing the wire.
const (
	OhlcvFunc           = "ohlcv"
	OhlcvStateFunc      = "ohlcv_state"
	OhlcvStateMergeFunc = "ohlcv_state_merge"
)

// ohlcvResolveDomain is the operator's refusal and its carrier decision in one
// place: an argument type the bar cannot be computed over is 42883 —
// PostgreSQL's undefined_function, the class it raises for an aggregate call
// whose argument types match no signature — and never a NULL bar.
//
// It is the RUNTIME half of the same question exec.OhlcvOutputFields answers
// at plan time; both go through OhlcvTimeInput / OhlcvDomainFor, so the
// declaration and the refusal cannot disagree.
func ohlcvResolveDomain(ts, price, vol *batch.Vector) (ohlcvDomain, error) {
	if !OhlcvTimeInput(parquet.TypeID(ts.Type)) {
		return ohlcvDomain{}, sqlerr.New("42883",
			"ohlcv(ts, price, volume): the first argument must be a TIMESTAMP or a DATE, not %v",
			ts.Type)
	}
	dom, ok := OhlcvDomainFor(parquet.TypeID(price.Type), price.DecimalData.Scale,
		parquet.TypeID(vol.Type), vol.DecimalData.Scale)
	if !ok {
		return ohlcvDomain{}, sqlerr.New("42883",
			"ohlcv(ts, price, volume): no bar over price %v and volume %v",
			price.Type, vol.Type)
	}
	return dom, nil
}
