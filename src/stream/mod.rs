// adjacency list representation of a graph
use std::{collections::HashMap, fmt::Debug};

#[derive(Debug)]
struct Vertex<T> {
    pub weight: T,

    edges: Vec<usize>,
}

#[derive(Debug, Clone, Copy)]
struct Edge<T> {
    pub weight: T,

    node: [usize; 2],
}

#[derive(Debug)]
struct Graph<N, E> {
    nodes: HashMap<usize, Vertex<N>>,
    edges: HashMap<usize, Edge<E>>,

    free_node_indices: Vec<usize>,
    free_edge_indices: Vec<usize>,
}

impl<N, E> Graph<N, E>
where
    N: Debug,
    E: Debug,
{
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            free_edge_indices: Vec::new(),
            free_node_indices: Vec::new(),
        }
    }

    fn get_free_node_index(&mut self) -> usize {
        if let Some(index) = self.free_node_indices.pop() {
            index
        } else {
            self.nodes.len()
        }
    }

    fn get_free_edge_index(&mut self) -> usize {
        if let Some(index) = self.free_edge_indices.pop() {
            index
        } else {
            self.edges.len()
        }
    }

    pub fn add_node(&mut self, weight: N) -> usize {
        let index = self.get_free_node_index();
        self.nodes.insert(
            index,
            Vertex {
                weight,
                edges: Vec::new(),
            },
        );
        index
    }

    pub fn get_node_weight(&self, index: usize) -> &N {
        &self.nodes.get(&index).unwrap().weight
    }

    pub fn get_weight_mut(&mut self, index: usize) -> &mut N {
        &mut self.nodes.get_mut(&index).unwrap().weight
    }

    pub fn add_edge(&mut self, weight: E, a: usize, b: usize) -> usize {
        let index = self.get_free_edge_index();
        let edge = Edge {
            weight,
            node: [a, b],
        };

        self.nodes.get_mut(&a).unwrap().edges.push(index);
        if a != b {
            self.nodes.get_mut(&b).unwrap().edges.push(index);
        }

        self.edges.insert(index, edge);
        index
    }

    pub fn remove_edge(&mut self, index: usize) {
        let edge = &self.edges.get(&index).unwrap();
        let a = edge.node[0];
        let b = edge.node[1];

        self.nodes
            .get_mut(&a)
            .unwrap()
            .edges
            .retain(|&i| i != index);

        if a != b {
            self.nodes
                .get_mut(&b)
                .unwrap()
                .edges
                .retain(|&i| i != index);
        }

        self.free_edge_indices.push(index);

        self.edges.remove(&index);
    }

    pub fn remove_node(&mut self, index: usize) {
        let edges = &self.nodes.get(&index).unwrap().edges.clone();
        for &edge_index in edges {
            self.remove_edge(edge_index);
        }

        self.free_node_indices.push(index);

        self.nodes.remove(&index);
    }

    pub fn get_neighbors(&self, index: usize) -> Vec<usize> {
        let mut neighbors = Vec::new();
        for &edge_index in &self.nodes.get(&index).unwrap().edges {
            let edge = &self.edges.get(&edge_index).unwrap();
            let neighbor_index = if edge.node[0] == index {
                edge.node[1]
            } else {
                edge.node[0]
            };
            neighbors.push(neighbor_index);
        }
        neighbors
    }

    pub fn get_directed_neighbors(&self, index: usize) -> Vec<usize> {
        let mut neighbors = Vec::new();
        for &edge_index in &self.nodes.get(&index).unwrap().edges {
            let edge = &self.edges.get(&edge_index).unwrap();
            if edge.node[0] == index {
                neighbors.push(edge.node[1]);
            }
        }
        neighbors
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_graph() {
        let mut graph = Graph::new();

        let a = graph.add_node("a");
        let b = graph.add_node("b");
        let c = graph.add_node("c");
        let d = graph.add_node("d");

        let _e1 = graph.add_edge((), a, b);
        let e2 = graph.add_edge((), a, c);
        let _e3 = graph.add_edge((), a, d);
        let _e4 = graph.add_edge((), b, c);
        let _e5 = graph.add_edge((), d, c);

        let mut neighbors_directed = graph.get_directed_neighbors(a);
        neighbors_directed.sort();
        assert_eq!(neighbors_directed.len(), 3);
        assert_eq!(neighbors_directed, vec![b, c, d]);

        let mut neighbors = graph.get_neighbors(b);
        neighbors.sort();
        assert_eq!(neighbors.len(), 2);
        assert_eq!(neighbors, vec![a, c]);

        graph.remove_edge(e2);

        let neighbors = graph.get_directed_neighbors(a);
        assert_eq!(neighbors.len(), 2);

        graph.remove_node(a);

        let mut neighbors = graph.get_neighbors(b);
        neighbors.sort();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors, vec![c]);
    }
}
