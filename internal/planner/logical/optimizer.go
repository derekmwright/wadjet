package logical

// Optimize applies logical optimizations to the plan tree.
func Optimize(plan *Node) *Node {
	plan = pushdownPredicates(plan)
	plan = pruneProjections(plan)
	return plan
}

// pushdownPredicates pushes filter predicates closer to scan nodes.
func pushdownPredicates(n *Node) *Node {
	if n == nil {
		return nil
	}

	// Recursively optimize children first
	for i, child := range n.Children {
		n.Children[i] = pushdownPredicates(child)
	}

	// If this is a Filter above a Scan, keep it (already at leaf)
	// If this is a Filter above a Project, swap them
	if n.Type == NodeFilter && len(n.Children) == 1 {
		child := n.Children[0]
		if child.Type == NodeProject {
			// Filter-Project -> Project-Filter (push filter below project)
			n.Children[0] = child.Children[0]
			child.Children[0] = n
			return child
		}
	}

	return n
}

// pruneProjections removes unnecessary projections (e.g., SELECT * passthrough).
func pruneProjections(n *Node) *Node {
	if n == nil {
		return nil
	}

	for i, child := range n.Children {
		n.Children[i] = pruneProjections(child)
	}

	// Remove identity projections (Project that just passes through all columns)
	if n.Type == NodeProject && len(n.Projections) == 0 && len(n.Children) == 1 {
		return n.Children[0]
	}

	return n
}
