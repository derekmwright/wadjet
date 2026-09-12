package expr

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// semverRowFields is the fixed allocation declaration for both parse forms.
func semverRowFields() []parquet.Column {
	return []parquet.Column{
		{Name: "major", Type: parquet.TypeInt64},
		{Name: "minor", Type: parquet.TypeInt64},
		{Name: "patch", Type: parquet.TypeInt64},
		{Name: "prerelease", Type: parquet.TypeString},
		{Name: "build", Type: parquet.TypeString},
	}
}

// semverParseRow parses once per call and builds all five fields from that
// result. The strict form changes only the invalid-string disposition.
func semverParseRow(args []any, strict bool) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		if strict {
			panic(fatalEval{errNotASemver("semver_parse_strict", toString(args[0]))})
		}
		return nil
	}
	return map[string]any{"major": v.major, "minor": v.minor, "patch": v.patch, "prerelease": v.pre, "build": v.build}
}
func fnSemverParse(args []any) any       { return semverParseRow(args, false) }
func fnSemverParseStrict(args []any) any { return semverParseRow(args, true) }
