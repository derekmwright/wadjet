package expr

import (
	"fmt"
	"strings"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
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

// UDFPersister is called after UDF register/unregister to persist the current state.
type UDFPersister func(udfs []UDFDef) error

// UDFStore holds compiled UDF definitions for use by the expression engine.
// Thread-safe for concurrent reads and writes.
type UDFStore struct {
	mu        sync.RWMutex
	udfs      map[string]*compiledUDF
	persister UDFPersister // optional: called after mutations to persist state
}

type compiledUDF struct {
	def  UDFDef
	body Expr     // compiled body expression (template — cloned per call site)
	deps []string // names of other UDFs this one depends on
}

// NewUDFStore creates a new empty UDF store.
func NewUDFStore() *UDFStore {
	return &UDFStore{udfs: make(map[string]*compiledUDF)}
}

// DefaultUDFs is the global UDF store.
var DefaultUDFs = NewUDFStore()

// SetPersister sets the function called after UDF mutations to persist state.
func (s *UDFStore) SetPersister(p UDFPersister) {
	s.mu.Lock()
	s.persister = p
	s.mu.Unlock()
}

// persist calls the persister with the current UDF definitions.
// Must be called with s.mu held (at least RLock).
func (s *UDFStore) persist() {
	if s.persister == nil {
		return
	}
	defs := make([]UDFDef, 0, len(s.udfs))
	for _, u := range s.udfs {
		defs = append(defs, u.def)
	}
	// Best-effort persistence — log errors but don't fail the operation.
	if err := s.persister(defs); err != nil {
		fmt.Printf("WARNING: failed to persist UDFs: %v\n", err)
	}
}

// LoadDefs registers pre-existing UDF definitions (e.g., from KV on startup).
// Skips compilation errors silently so one bad UDF doesn't block startup.
func (s *UDFStore) LoadDefs(defs []UDFDef) int {
	loaded := 0
	for _, def := range defs {
		if err := s.Register(def, true); err == nil {
			loaded++
		}
	}
	return loaded
}

// Register compiles and registers a UDF.
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

	// Check for circular dependency
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

	s.mu.Lock()
	s.udfs[def.Name] = &compiledUDF{def: def, body: body, deps: deps}
	s.persist()
	s.mu.Unlock()

	DefaultRegistry.Register(def.Name, s.makeScalarFunc(def.Name))

	return nil
}

// isBuiltinFunc checks if a function name is a builtin.
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

// Unregister removes a UDF.
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
	s.persist()
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

// CompileUDFCall creates a UDFCall expression node.
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

func (s *UDFStore) makeScalarFunc(name string) ScalarFunc {
	return func(args []any) any {
		s.mu.RLock()
		u, ok := s.udfs[name]
		s.mu.RUnlock()
		if !ok {
			return nil
		}

		argsSlice := make([]any, len(args))
		copy(argsSlice, args)
		argsPtr := &argsSlice

		body := cloneExprWithArgs(u.body, argsPtr)
		return body.Eval(nil, 0)
	}
}

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
// with ParamRef nodes.
func compileUDFBody(body string, params []string) (Expr, error) {
	node, err := plansql.ParseExpression(body)
	if err != nil {
		return nil, fmt.Errorf("parsing body expression: %w", err)
	}

	// Build param name → index map
	paramIdx := make(map[string]int, len(params))
	for i, p := range params {
		paramIdx[strings.ToLower(p)] = i
	}

	return compileWithParamRefs(node, paramIdx)
}

// compileWithParamRefs compiles our AST, substituting params with ParamRef nodes.
func compileWithParamRefs(node plansql.Node, paramIdx map[string]int) (Expr, error) {
	if node == nil {
		return &Lit{Val: nil}, nil
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		name := strings.ToLower(n.Column)
		if idx, ok := paramIdx[name]; ok {
			return &ParamRef{Index: idx}, nil
		}
		return &ColRef{Name: n.Column}, nil

	case *plansql.Lit:
		return compileLit(n)

	case *plansql.StarNode:
		return &Lit{Val: "*"}, nil

	case *plansql.BinaryOp:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		return &BinOp{Left: left, Right: right, Op: n.Op}, nil

	case *plansql.UnaryOp:
		operand, err := compileWithParamRefs(n.Inner, paramIdx)
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Operand: operand, Op: n.Op}, nil

	case *plansql.CmpExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		var op CmpOp
		switch n.Op {
		case "=":
			op = CmpEq
		case "!=", "<>":
			op = CmpNe
		case "<":
			op = CmpLt
		case "<=":
			op = CmpLe
		case ">":
			op = CmpGt
		case ">=":
			op = CmpGe
		default:
			op = CmpEq
		}
		return &Cmp{Left: left, Right: right, Op: op}, nil

	case *plansql.InExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		var values []Expr
		for _, v := range n.Values {
			compiled, err := compileWithParamRefs(v, paramIdx)
			if err != nil {
				return nil, err
			}
			values = append(values, compiled)
		}
		return &In{Expr: left, Values: values, Not: n.Not}, nil

	case *plansql.BetweenExpr:
		expr, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		low, err := compileWithParamRefs(n.Low, paramIdx)
		if err != nil {
			return nil, err
		}
		hi, err := compileWithParamRefs(n.High, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Between{Expr: expr, Low: low, Hi: hi, Not: n.Not}, nil

	case *plansql.LikeExpr:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Pattern, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Like{Expr: left, Pattern: right, Not: n.Not}, nil

	case *plansql.AndNode:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		return &And{Left: left, Right: right}, nil

	case *plansql.OrNode:
		left, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithParamRefs(n.Right, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Or{Left: left, Right: right}, nil

	case *plansql.NotNode:
		operand, err := compileWithParamRefs(n.Inner, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil

	case *plansql.ParenNode:
		return compileWithParamRefs(n.Inner, paramIdx)

	case *plansql.FuncCallNode:
		name := strings.ToLower(n.Name)
		var args []Expr
		if n.Star {
			args = append(args, &Lit{Val: "*"})
		} else {
			for _, arg := range n.Args {
				compiled, err := compileWithParamRefs(arg, paramIdx)
				if err != nil {
					return nil, err
				}
				args = append(args, compiled)
			}
		}
		if name == "coalesce" {
			return &Coalesce{Args: args}, nil
		}
		return &FuncCall{Name: name, Args: args}, nil

	case *plansql.CaseNode:
		c := &Case{}
		if n.Subject != nil {
			var err error
			c.Operand, err = compileWithParamRefs(n.Subject, paramIdx)
			if err != nil {
				return nil, err
			}
		}
		for _, when := range n.Whens {
			cond, err := compileWithParamRefs(when.Cond, paramIdx)
			if err != nil {
				return nil, err
			}
			result, err := compileWithParamRefs(when.Result, paramIdx)
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

	case *plansql.IsExpr:
		operand, err := compileWithParamRefs(n.Left, paramIdx)
		if err != nil {
			return nil, err
		}
		switch n.Check {
		case "null":
			return &IsNull{Operand: operand, Not: n.Not}, nil
		default:
			return nil, fmt.Errorf("unsupported IS operator in UDF: %s", n.Check)
		}

	case *plansql.CastNode:
		operand, err := compileWithParamRefs(n.Inner, paramIdx)
		if err != nil {
			return nil, err
		}
		return &Cast{Operand: operand, DestType: strings.ToLower(n.TypeName)}, nil

	default:
		return &Lit{Val: node.String()}, nil
	}
}

// extractFuncDeps extracts names of function calls from a SQL expression body.
func extractFuncDeps(body string) ([]string, error) {
	node, err := plansql.ParseExpression(body)
	if err != nil {
		return nil, err
	}
	var deps []string
	walkFuncDeps(node, &deps)
	return deps, nil
}

func walkFuncDeps(node plansql.Node, deps *[]string) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		*deps = append(*deps, strings.ToLower(n.Name))
		for _, arg := range n.Args {
			walkFuncDeps(arg, deps)
		}
	case *plansql.BinaryOp:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *plansql.UnaryOp:
		walkFuncDeps(n.Inner, deps)
	case *plansql.CmpExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *plansql.AndNode:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *plansql.OrNode:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Right, deps)
	case *plansql.NotNode:
		walkFuncDeps(n.Inner, deps)
	case *plansql.ParenNode:
		walkFuncDeps(n.Inner, deps)
	case *plansql.CaseNode:
		if n.Subject != nil {
			walkFuncDeps(n.Subject, deps)
		}
		for _, w := range n.Whens {
			walkFuncDeps(w.Cond, deps)
			walkFuncDeps(w.Result, deps)
		}
		if n.Else != nil {
			walkFuncDeps(n.Else, deps)
		}
	case *plansql.IsExpr:
		walkFuncDeps(n.Left, deps)
	case *plansql.BetweenExpr:
		walkFuncDeps(n.Left, deps)
		walkFuncDeps(n.Low, deps)
		walkFuncDeps(n.High, deps)
	}
}

// cloneExprWithArgs deep-clones an expression tree, replacing all ParamRef
// nodes' args pointer with the given pointer.
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
		return e
	}
}
