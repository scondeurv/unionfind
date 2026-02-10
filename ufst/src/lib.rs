//! # Union-Find Algorithm Library
//!
//! This library implements the Union-Find (Disjoint Set Union) algorithm for finding
//! connected components in an undirected graph.
//!
//! ## Features
//!
//! - **Path Compression**: Flattens tree structure for O(α(n)) amortized operations
//! - **Union by Rank**: Keeps trees balanced for optimal performance
//! - **Memory Efficient**: Uses compact array representation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Union-Find data structure with path compression and union by rank
pub struct UnionFind {
    /// Parent pointers (parent[i] = parent of node i)
    parent: Vec<u32>,
    /// Rank for union by rank optimization
    rank: Vec<u32>,
}

impl UnionFind {
    /// Creates a new Union-Find structure for n nodes
    /// Initially, each node is its own parent (singleton components)
    pub fn new(n: u32) -> Self {
        let parent: Vec<u32> = (0..n).collect();
        let rank = vec![0; n as usize];
        UnionFind { parent, rank }
    }

    /// Finds the root/representative of the component containing node x
    /// Uses path compression: all nodes along the path point directly to root
    pub fn find(&mut self, mut x: u32) -> u32 {
        if (x as usize) >= self.parent.len() {
            panic!("Node index {} out of bounds (max {})", x, self.parent.len() - 1);
        }
        let mut root = x;
        // Find the root
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        
        // Path compression: make all nodes on path point to root
        while self.parent[x as usize] != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }

    /// Unions the components containing nodes x and y
    /// Uses union by rank: smaller tree goes under larger tree
    /// Returns true if a union was performed (they were in different components)
    pub fn union(&mut self, x: u32, y: u32) -> bool {
        let root_x = self.find(x);
        let root_y = self.find(y);

        if root_x == root_y {
            return false; // Already in same component
        }

        // Union by rank: attach smaller tree under larger tree
        let rank_x = self.rank[root_x as usize];
        let rank_y = self.rank[root_y as usize];

        if rank_x < rank_y {
            self.parent[root_x as usize] = root_y;
        } else if rank_x > rank_y {
            self.parent[root_y as usize] = root_x;
        } else {
            // Same rank: arbitrarily choose x as root, increment its rank
            self.parent[root_y as usize] = root_x;
            self.rank[root_x as usize] += 1;
        }

        true
    }

    /// Returns the parent array (after full path compression)
    pub fn get_parents(&mut self) -> Vec<u32> {
        self.get_parents_and_count().0
    }

    /// Counts the number of distinct connected components
    pub fn count_components(&mut self) -> usize {
        self.get_parents_and_count().1
    }

    /// Returns the parent array and component count in a single pass (Efficient)
    pub fn get_parents_and_count(&mut self) -> (Vec<u32>, usize) {
        let n = self.parent.len() as u32;
        let mut roots = std::collections::HashSet::new();
        for i in 0..n {
            roots.insert(self.find(i));
        }
        (self.parent.clone(), roots.len())
    }

    /// Returns a map from component root to list of nodes in that component
    pub fn get_components(&mut self) -> HashMap<u32, Vec<u32>> {
        let n = self.parent.len() as u32;
        let mut components: HashMap<u32, Vec<u32>> = HashMap::new();
        for i in 0..n {
            let root = self.find(i);
            components.entry(root).or_default().push(i);
        }
        components
    }
}

/// Run Union-Find on a graph represented as an adjacency list
/// Returns the parent array where parent[i] is the root of node i's component
pub fn run_union_find(adj: &HashMap<u32, Vec<u32>>, num_nodes: u32) -> Vec<u32> {
    let mut uf = UnionFind::new(num_nodes);

    // Process all edges
    for (&src, neighbors) in adj {
        for &dst in neighbors {
            uf.union(src, dst);
        }
    }

    uf.get_parents()
}

/// Run Union-Find on a list of edges
/// Returns a UnionFindResult with parents, component count, and timing
pub fn run_union_find_edges(edges: &[(u32, u32)], num_nodes: u32) -> UnionFindResult {
    let start_time = Instant::now();
    let mut uf = UnionFind::new(num_nodes);

    for &(src, dst) in edges {
        uf.union(src, dst);
    }

    let (parents, num_components) = uf.get_parents_and_count();
    let execution_time_ms = start_time.elapsed().as_millis();

    UnionFindResult {
        parent: parents,
        num_components,
        load_time_ms: 0,
        execution_time_ms,
        total_time_ms: execution_time_ms,
    }
}

/// Result of the Union-Find algorithm
#[derive(Debug, Serialize, Deserialize)]
pub struct UnionFindResult {
    /// Parent array (parent[i] = root of component containing node i)
    pub parent: Vec<u32>,
    /// Number of connected components found
    pub num_components: usize,
    /// Time to load graph (ms)
    pub load_time_ms: u128,
    /// Time to run algorithm (ms)
    pub execution_time_ms: u128,
    /// Total time (ms)
    pub total_time_ms: u128,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton_components() {
        let mut uf = UnionFind::new(5);
        assert_eq!(uf.count_components(), 5);
        for i in 0..5 {
            assert_eq!(uf.find(i), i);
        }
    }

    #[test]
    fn test_union_and_find() {
        let mut uf = UnionFind::new(5);
        uf.union(0, 1);
        uf.union(2, 3);
        
        assert_eq!(uf.find(0), uf.find(1));
        assert_eq!(uf.find(2), uf.find(3));
        assert_ne!(uf.find(0), uf.find(2));
        assert_eq!(uf.count_components(), 3); // {0,1}, {2,3}, {4}
    }

    #[test]
    fn test_transitive_union() {
        let mut uf = UnionFind::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(3, 4);
        
        // 0, 1, 2 should be in same component
        assert_eq!(uf.find(0), uf.find(2));
        // 3, 4 should be in same component
        assert_eq!(uf.find(3), uf.find(4));
        // Two separate components
        assert_ne!(uf.find(0), uf.find(3));
        assert_eq!(uf.count_components(), 2);
    }

    #[test]
    fn test_run_union_find_edges() {
        let edges = vec![(0, 1), (1, 2), (3, 4)];
        let result = run_union_find_edges(&edges, 5);
        
        assert_eq!(result.num_components, 2);
        // Nodes 0, 1, 2 should have same root
        assert_eq!(result.parent[0], result.parent[1]);
        assert_eq!(result.parent[1], result.parent[2]);
        // Nodes 3, 4 should have same root
        assert_eq!(result.parent[3], result.parent[4]);
    }

    #[test]
    fn test_duplicate_and_self_loop() {
        let mut uf = UnionFind::new(3);
        uf.union(0, 0); // self-loop
        uf.union(0, 1);
        uf.union(0, 1); // duplicate
        
        assert_eq!(uf.count_components(), 2); // {0,1}, {2}
    }
}
