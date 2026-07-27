package harness

import (
	"math/rand/v2"
	"testing"
)

// The gate's two contractual properties: float summation-order wobble
// passes, value inflation (the #278 / eager-§14.3 corruption class) fails.
func TestValueSigWobbleVsInflation(t *testing.T) {
	vals := make([]float64, 10000)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range vals {
		vals[i] = r.Float64() * 1e9
	}

	var fwd, rev ValueSigAccum
	for _, v := range vals {
		fwd.AddFloat(0, v)
	}
	for i := len(vals) - 1; i >= 0; i-- {
		rev.AddFloat(0, vals[i])
	}
	if ok, detail := CompareValueSigs(fwd.Signature(), rev.Signature(), ValueSigRelTol); !ok {
		t.Fatalf("summation-order wobble must pass the tolerance: %s", detail)
	}

	// 0.01% inflation (vastly smaller than the 2x/3x incident classes)
	// must still fail.
	var inflated ValueSigAccum
	for _, v := range vals {
		inflated.AddFloat(0, v*1.0001)
	}
	if ok, _ := CompareValueSigs(fwd.Signature(), inflated.Signature(), ValueSigRelTol); ok {
		t.Fatal("0.01% inflation must fail the tolerance")
	}
}

func TestValueSigAddValsTypes(t *testing.T) {
	var a ValueSigAccum
	a.AddVals([]any{int64(2), "3.5", []byte("4.5"), "not-a-number", float32(1)})
	a.AddVals([]any{int64(8), "6.5", []byte("5.5"), "still-not", float32(2)})
	sig := a.Signature()
	want := "c0:1.000000000e+01,c1:1.000000000e+01,c2:1.000000000e+01,c4:3.000000000e+00"
	if sig != want {
		t.Fatalf("signature = %q, want %q (non-numeric column omitted)", sig, want)
	}
}

func TestCompareValueSigsShape(t *testing.T) {
	if ok, _ := CompareValueSigs("c0:1.0e+00", "c0:1.0e+00,c1:2.0e+00", ValueSigRelTol); ok {
		t.Fatal("differing column sets must fail")
	}
	if ok, _ := CompareValueSigs("", "", ValueSigRelTol); !ok {
		t.Fatal("two empty signatures must pass")
	}
	if ok, detail := CompareValueSigs("garbage", "c0:1.0", ValueSigRelTol); ok || detail == "" {
		t.Fatal("unparseable baseline must fail with detail")
	}
}
