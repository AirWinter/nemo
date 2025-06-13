use std::collections::{HashMap, HashSet};

/// Data structure used to build a dependency graph between transactions.
pub(crate) struct DependencyGraph {
    // Outgoing edges: index -> set of dependencies it points to
    edges: HashMap<usize, HashSet<usize>>,

    // Incoming edges: index -> set of things that depend on this
    reverse_edges: HashMap<usize, HashSet<usize>>,
}

impl DependencyGraph {
    pub fn new() -> DependencyGraph {
        DependencyGraph {
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
        }
    }

    /// Method to add a dependency between two transaction indices.
    pub fn add_dependency(&mut self, from: usize, to: usize) {
        self.edges.entry(from).or_default().insert(to);
        self.reverse_edges.entry(to).or_default().insert(from);
    }

    /// Method to clear all dependencies that were blocked by `from` transaction index. Returns a
    /// vector of transaction indices for transactions that no longer have a dependency and are now
    /// ready to be executed.
    pub fn remove_dependency(&mut self, from: &usize) -> Vec<usize> {
        // Collect all nodes `from` points to
        let dependents = match self.edges.get_mut(from) {
            Some(set) => set.drain().collect::<Vec<_>>(),
            None => return vec![], // No dependencies to remove
        };

        let mut freed_nodes: Vec<usize> = Vec::new();

        // For each dependent, remove `from` from their reverse edges
        for dep in dependents {
            if let Some(parents) = self.reverse_edges.get_mut(&dep) {
                parents.remove(from);
                if parents.is_empty() {
                    freed_nodes.push(dep);
                }
            }
        }
        freed_nodes
    }

    /// Method to check whether the transaction corresponding with `index` has any dependencies. If
    /// it does (return true) indicating it isn't ready to be executed, otherwise (return false) it
    /// is ready to be executed.
    pub fn has_dependencies(&self, index: usize) -> bool {
        self.reverse_edges
            .get(&index)
            .map_or(false, |s| !s.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use crate::iota::dependency_graph::DependencyGraph;
    use std::collections::HashSet;

    #[test]
    fn it_works() {
        let mut dep_graph = DependencyGraph::new();
        // Check that add_dependency works as intended
        assert!(!dep_graph.has_dependencies(0));
        assert!(!dep_graph.has_dependencies(1));
        dep_graph.add_dependency(0, 1);
        assert!(dep_graph.has_dependencies(1));
        assert!(!dep_graph.has_dependencies(0));
        assert!(
            dep_graph.edges.get(&0).map_or(false, |s| s.contains(&1)),
            "0 should have a dependency on 1"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&1)
                .map_or(false, |s| s.contains(&0)),
            "1 should have 0 as a reverse dependency"
        );
        assert!(
            dep_graph.edges.get(&1).map_or(true, |s| !s.contains(&0)),
            "1 should not have a dependency on 0"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&0)
                .map_or(true, |s| !s.contains(&1)),
            "0 should not have 1 as a reverse dependency"
        );

        dep_graph.add_dependency(0, 2);
        assert!(
            dep_graph.edges.get(&0).map_or(false, |s| s.contains(&2)),
            "0 should have a dependency on 2"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&2)
                .map_or(false, |s| s.contains(&0)),
            "2 should have 0 as a reverse dependency"
        );
        assert!(
            dep_graph.edges.get(&2).map_or(true, |s| !s.contains(&0)),
            "2 should not have a dependency on 0"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&0)
                .map_or(true, |s| !s.contains(&2)),
            "0 should not have 2 as a reverse dependency"
        );

        dep_graph.add_dependency(1, 3);
        assert!(
            dep_graph.edges.get(&1).map_or(false, |s| s.contains(&3)),
            "1 should have a dependency on 3"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&3)
                .map_or(false, |s| s.contains(&1)),
            "3 should have 1 as a reverse dependency"
        );
        assert!(
            dep_graph.edges.get(&3).map_or(true, |s| !s.contains(&1)),
            "3 should not have a dependency on 1"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&1)
                .map_or(true, |s| !s.contains(&3)),
            "1 should not have 3 as a reverse dependency"
        );

        dep_graph.add_dependency(2, 3);
        assert!(
            dep_graph.edges.get(&2).map_or(false, |s| s.contains(&3)),
            "2 should have a dependency on 3"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&3)
                .map_or(false, |s| s.contains(&2)),
            "3 should have 2 as a reverse dependency"
        );
        assert!(
            dep_graph.edges.get(&3).map_or(true, |s| !s.contains(&2)),
            "3 should not have a dependency on 2"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&2)
                .map_or(true, |s| !s.contains(&3)),
            "2 should not have 3 as a reverse dependency"
        );

        // Check that remove_dependency works as intended
        let result: HashSet<_> = dep_graph.remove_dependency(&0).into_iter().collect();
        let expected: HashSet<_> = vec![1, 2].into_iter().collect();
        assert_eq!(result, expected);
        assert!(
            dep_graph.edges.get(&0).map_or(true, |s| !s.contains(&1)),
            "0 should no longer have a dependency on 1"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&1)
                .map_or(true, |s| !s.contains(&0)),
            "1 should no longer have 0 as a reverse dependency"
        );

        assert!(dep_graph.remove_dependency(&1).is_empty());
        assert!(
            dep_graph.edges.get(&1).map_or(true, |s| !s.contains(&3)),
            "1 should no longer have a dependency on 3"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&3)
                .map_or(true, |s| !s.contains(&1)),
            "3 should no longer have 1 as a reverse dependency"
        );

        assert_eq!(dep_graph.remove_dependency(&2), vec![3]);
        assert!(
            dep_graph.edges.get(&2).map_or(true, |s| !s.contains(&3)),
            "2 should no longer have a dependency on 3"
        );
        assert!(
            dep_graph
                .reverse_edges
                .get(&3)
                .map_or(true, |s| !s.contains(&2)),
            "3 should no longer have 2 as a reverse dependency"
        );
    }
}
