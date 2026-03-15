// Package sql provides SQL parsing using vitess-sqlparser.
package sql

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// ParsedQuery represents a parsed SQL query.
type ParsedQuery struct {
	AST       sqlparser.Statement
	Type      QueryType
	TableName string
	SQL       string
	Explain   *ExplainInfo
	Describe  *DescribeInfo
}

// ExplainInfo holds details for an EXPLAIN statement.
type ExplainInfo struct {
	Verbose  bool
	InnerSQL string
}

// DescribeInfo holds details for a DESCRIBE/SHOW COLUMNS statement.
type DescribeInfo struct {
	TableName string
}

// QueryType identifies the kind of SQL statement.
type QueryType int

const (
	QuerySelect QueryType = iota
	QueryExplain
	QueryDescribe
	QueryUnsupported
)

// Parse parses a SQL string into a ParsedQuery.
func Parse(sql string) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	// Pre-parse: EXPLAIN [VERBOSE] <query>
	if strings.HasPrefix(upper, "EXPLAIN ") {
		return parseExplain(trimmed)
	}

	// Pre-parse: DESCRIBE <table> / DESC <table>
	if strings.HasPrefix(upper, "DESCRIBE ") || strings.HasPrefix(upper, "DESC ") {
		return parseDescribe(trimmed)
	}

	// Pre-parse: SHOW COLUMNS FROM <table>
	if strings.HasPrefix(upper, "SHOW COLUMNS FROM ") {
		return parseShowColumns(trimmed)
	}

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	pq := &ParsedQuery{
		AST: stmt,
		SQL: sql,
	}

	switch stmt.(type) {
	case *sqlparser.Select:
		pq.Type = QuerySelect
	default:
		pq.Type = QueryUnsupported
		return pq, fmt.Errorf("unsupported statement type: %T", stmt)
	}

	return pq, nil
}

func parseExplain(sql string) (*ParsedQuery, error) {
	// Strip "EXPLAIN"
	rest := strings.TrimSpace(sql[len("EXPLAIN"):])
	upper := strings.ToUpper(rest)

	verbose := false
	if strings.HasPrefix(upper, "VERBOSE ") {
		verbose = true
		rest = strings.TrimSpace(rest[len("VERBOSE"):])
	}

	// Parse the inner SQL as a normal SELECT
	inner, err := Parse(rest)
	if err != nil {
		return nil, fmt.Errorf("parsing EXPLAIN query: %w", err)
	}

	return &ParsedQuery{
		AST:  inner.AST,
		Type: QueryExplain,
		SQL:  sql,
		Explain: &ExplainInfo{
			Verbose:  verbose,
			InnerSQL: rest,
		},
	}, nil
}

func parseDescribe(sql string) (*ParsedQuery, error) {
	// Strip "DESCRIBE" or "DESC"
	upper := strings.ToUpper(sql)
	var rest string
	if strings.HasPrefix(upper, "DESCRIBE ") {
		rest = strings.TrimSpace(sql[len("DESCRIBE"):])
	} else {
		rest = strings.TrimSpace(sql[len("DESC"):])
	}

	tableName := strings.TrimRight(rest, ";")
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, fmt.Errorf("DESCRIBE requires a table name")
	}

	return &ParsedQuery{
		Type: QueryDescribe,
		SQL:  sql,
		Describe: &DescribeInfo{
			TableName: tableName,
		},
	}, nil
}

func parseShowColumns(sql string) (*ParsedQuery, error) {
	rest := strings.TrimSpace(sql[len("SHOW COLUMNS FROM"):])
	tableName := strings.TrimRight(rest, ";")
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, fmt.Errorf("SHOW COLUMNS FROM requires a table name")
	}

	return &ParsedQuery{
		Type: QueryDescribe,
		SQL:  sql,
		Describe: &DescribeInfo{
			TableName: tableName,
		},
	}, nil
}

// ExtractSelect extracts details from a SELECT statement.
func ExtractSelect(pq *ParsedQuery) (*SelectInfo, error) {
	sel, ok := pq.AST.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	info := &SelectInfo{}

	// Extract FROM tables
	for _, expr := range sel.From {
		switch t := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			name, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}
			ti := TableRef{
				Name:  name.Name.String(),
				Alias: t.As.String(),
			}
			if ti.Alias == "" {
				ti.Alias = ti.Name
			}
			info.Tables = append(info.Tables, ti)
		case *sqlparser.JoinTableExpr:
			info.Joins = append(info.Joins, extractJoin(t))
			// Also extract the left table
			if alias, ok := t.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
				name, ok := alias.Expr.(sqlparser.TableName)
				if ok {
					ti := TableRef{
						Name:  name.Name.String(),
						Alias: alias.As.String(),
					}
					if ti.Alias == "" {
						ti.Alias = ti.Name
					}
					info.Tables = append(info.Tables, ti)
				}
			}
		}
	}

	// Extract SELECT columns
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			info.Columns = append(info.Columns, SelectColumn{
				Expr: "*",
				Star: true,
			})
		case *sqlparser.AliasedExpr:
			col := SelectColumn{
				Expr:    sqlparser.String(e.Expr),
				Alias:   e.As.String(),
				ASTExpr: e.Expr,
			}
			// Check if it's an aggregate function
			if fn, ok := e.Expr.(*sqlparser.FuncExpr); ok {
				col.IsAgg = true
				col.AggFunc = fn.Name.Lowered()
				if len(fn.Exprs) > 0 {
					col.AggArg = sqlparser.String(fn.Exprs[0])
				}
			}
			// Check if it's a simple column reference
			if colName, ok := e.Expr.(*sqlparser.ColName); ok {
				col.ColumnRef = colName.Name.String()
				col.TableRef = colName.Qualifier.Name.String()
			}
			info.Columns = append(info.Columns, col)
		}
	}

	// Extract WHERE
	if sel.Where != nil {
		info.Where = sqlparser.String(sel.Where.Expr)
		info.WhereExpr = sel.Where.Expr
	}

	// Extract GROUP BY
	for _, expr := range sel.GroupBy {
		info.GroupBy = append(info.GroupBy, sqlparser.String(expr))
	}

	// Extract HAVING
	if sel.Having != nil {
		info.Having = sqlparser.String(sel.Having.Expr)
	}

	// Extract ORDER BY
	for _, order := range sel.OrderBy {
		info.OrderBy = append(info.OrderBy, OrderByItem{
			Column: sqlparser.String(order.Expr),
			Desc:   order.Direction == sqlparser.DescScr,
		})
	}

	// Extract LIMIT
	if sel.Limit != nil {
		if sel.Limit.Rowcount != nil {
			info.Limit = sqlparser.String(sel.Limit.Rowcount)
		}
		if sel.Limit.Offset != nil {
			info.Offset = sqlparser.String(sel.Limit.Offset)
		}
	}

	return info, nil
}

// SelectInfo contains extracted information from a SELECT statement.
type SelectInfo struct {
	Tables    []TableRef
	Joins     []JoinInfo
	Columns   []SelectColumn
	Where     string
	WhereExpr sqlparser.Expr
	GroupBy   []string
	Having    string
	OrderBy  []OrderByItem
	Limit    string
	Offset   string
}

// TableRef is a reference to a table.
type TableRef struct {
	Name  string
	Alias string
}

// SelectColumn describes a column in a SELECT clause.
type SelectColumn struct {
	Expr      string
	Alias     string
	Star      bool
	IsAgg     bool
	AggFunc   string
	AggArg    string
	ColumnRef string
	TableRef  string
	ASTExpr   sqlparser.Expr // original AST expression node
}

// JoinInfo describes a JOIN clause.
type JoinInfo struct {
	Type      string // inner, left, right
	LeftTable string
	RightTable string
	RightAlias string
	Condition  string
	CondExpr   sqlparser.Expr
}

// OrderByItem describes an ORDER BY element.
type OrderByItem struct {
	Column string
	Desc   bool
}

func extractJoin(join *sqlparser.JoinTableExpr) JoinInfo {
	ji := JoinInfo{
		Type: join.Join,
	}

	if alias, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		if name, ok := alias.Expr.(sqlparser.TableName); ok {
			ji.RightTable = name.Name.String()
			ji.RightAlias = alias.As.String()
			if ji.RightAlias == "" {
				ji.RightAlias = ji.RightTable
			}
		}
	}

	if join.On != nil {
		ji.Condition = sqlparser.String(join.On)
		ji.CondExpr = join.On
	}

	return ji
}
