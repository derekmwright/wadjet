package sql

// WindowSpec describes a window function specification.
type WindowSpec struct {
	FuncName    string
	Args        string // raw arg string (e.g., "amount", "*", "")
	PartitionBy []string
	OrderBy     []WindowOrderItem
	Alias       string // output column name
	Frame       *WindowFrame // optional frame specification
}

// WindowOrderItem describes a column + direction in a window ORDER BY.
type WindowOrderItem struct {
	Column     string
	Desc       bool
	NullsFirst *bool
}
