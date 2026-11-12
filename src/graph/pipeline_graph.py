"""
Pipeline Dependency Graph Engine
================================
Uses NetworkX to construct a Directed Acyclic Graph (DAG) modeling both:
  - Dataset nodes (e.g. 'raw.customers', 'clean.orders')
  - Job nodes (e.g. 'job_ingest_customers', 'job_build_sales_summary')

Key capabilities:
  1. Build graph dynamically from the `pipeline_jobs` table in PostgreSQL.
  2. Upstream lineage traversal: Find all ancestors that feed a broken dataset.
  3. Downstream impact analysis: Find the "blast radius" if a job or dataset fails.
  4. Path discovery: Find the exact execution path between any two nodes.
"""

import sys
import json
from pathlib import Path
from typing import Set, List, Dict, Any, Optional
import networkx as nx

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import fetch_all


class PipelineGraph:
    """
    Manages the DAG of data pipeline jobs and datasets.
    """

    def __init__(self):
        # DiGraph = Directed Graph (edges have direction A -> B)
        self.graph = nx.DiGraph()
        self.load_from_db()

    def load_from_db(self) -> None:
        """
        Queries the `pipeline_jobs` table and populates the graph.
        Creates bipartite edges:
            Input Dataset -> Job -> Output Dataset
        """
        self.graph.clear()

        jobs = fetch_all(
            "SELECT job_id, job_name, input_datasets, output_datasets, owner FROM pipeline_jobs"
        )

        for job in jobs:
            job_id = job["job_id"]
            
            # 1. Add Job Node
            self.graph.add_node(
                job_id,
                node_type="job",
                name=job["job_name"],
                owner=job.get("owner", "unknown")
            )

            # Parse inputs & outputs
            inputs = job["input_datasets"]
            if isinstance(inputs, str):
                inputs = json.loads(inputs)

            outputs = job["output_datasets"]
            if isinstance(outputs, str):
                outputs = json.loads(outputs)

            # 2. Add Input Datasets & Edges: (input_dataset) -> [job]
            for in_ds in inputs:
                if not self.graph.has_node(in_ds):
                    self.graph.add_node(in_ds, node_type="dataset", name=in_ds)
                self.graph.add_edge(in_ds, job_id, relationship="read_by")

            # 3. Add Output Datasets & Edges: [job] -> (output_dataset)
            for out_ds in outputs:
                if not self.graph.has_node(out_ds):
                    self.graph.add_node(out_ds, node_type="dataset", name=out_ds)
                self.graph.add_edge(job_id, out_ds, relationship="produces")

    def get_upstream_ancestors(self, node: str, node_type: Optional[str] = None) -> List[str]:
        """
        Finds all upstream predecessors (lineage) that feed into `node`.
        
        Parameters:
            node: Node identifier (e.g. 'analytics.customer_sales_summary')
            node_type: Filter by 'job', 'dataset', or None (both)
            
        Returns:
            List of upstream node IDs sorted topologically (deepest upstream first)
        """
        if not self.graph.has_node(node):
            return []

        # nx.ancestors returns all nodes that have a path directed to `node`
        ancestors = nx.ancestors(self.graph, node)

        # Sort topologically: upstream sources come before downstream consumers
        subgraph = self.graph.subgraph(ancestors.union({node}))
        sorted_nodes = [n for n in nx.topological_sort(subgraph) if n != node]

        if node_type:
            sorted_nodes = [
                n for n in sorted_nodes
                if self.graph.nodes[n].get("node_type") == node_type
            ]

        return sorted_nodes

    def get_downstream_impact(self, node: str, node_type: Optional[str] = None) -> List[str]:
        """
        Finds the "blast radius": all downstream nodes affected if `node` is faulty.
        
        Returns:
            List of downstream node IDs sorted topologically (immediate dependents first)
        """
        if not self.graph.has_node(node):
            return []

        descendants = nx.descendants(self.graph, node)
        subgraph = self.graph.subgraph(descendants.union({node}))
        sorted_nodes = [n for n in nx.topological_sort(subgraph) if n != node]

        if node_type:
            sorted_nodes = [
                n for n in sorted_nodes
                if self.graph.nodes[n].get("node_type") == node_type
            ]

        return sorted_nodes

    def get_direct_parents(self, node: str) -> List[str]:
        """Returns direct 1-step predecessors."""
        if not self.graph.has_node(node):
            return []
        return list(self.graph.predecessors(node))

    def get_direct_children(self, node: str) -> List[str]:
        """Returns direct 1-step successors."""
        if not self.graph.has_node(node):
            return []
        return list(self.graph.successors(node))

    def find_paths(self, source: str, target: str) -> List[List[str]]:
        """Finds all simple paths between source and target."""
        if not self.graph.has_node(source) or not self.graph.has_node(target):
            return []
        try:
            return list(nx.all_simple_paths(self.graph, source, target))
        except nx.NetworkXNoPath:
            return []

    def get_root_sources(self) -> List[str]:
        """Returns entry-point nodes (nodes with in-degree == 0, like raw files)."""
        return [n for n, in_degree in self.graph.in_degree() if in_degree == 0]

    def get_leaf_datasets(self) -> List[str]:
        """Returns final end-product datasets (out-degree == 0, like analytics tables)."""
        return [
            n for n, out_degree in self.graph.out_degree()
            if out_degree == 0 and self.graph.nodes[n].get("node_type") == "dataset"
        ]

    def to_mermaid(self) -> str:
        """Generates a Mermaid graph string for documentation or dashboard rendering."""
        lines = ["flowchart TD"]
        for u, v in self.graph.edges():
            u_type = self.graph.nodes[u].get("node_type", "dataset")
            v_type = self.graph.nodes[v].get("node_type", "dataset")

            u_repr = f'{u}["{u}"]' if u_type == "job" else f'{u}(("{u}"))'
            v_repr = f'{v}["{v}"]' if v_type == "job" else f'{v}(("{v}"))'

            lines.append(f"    {u_repr} --> {v_repr}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Self-test if executed directly
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("TESTING PIPELINE GRAPH ENGINE")
    print("=" * 60)

    pg = PipelineGraph()

    print(f"\nTotal Nodes in Graph: {pg.graph.number_of_nodes()}")
    print(f"Total Edges in Graph: {pg.graph.number_of_edges()}")

    print("\nRoot Sources (Entry points):", pg.get_root_sources())
    print("Leaf Datasets (End outputs):", pg.get_leaf_datasets())

    target = "analytics.customer_sales_summary"
    print(f"\nLineage for: {target}")
    print("  Upstream Jobs:", pg.get_upstream_ancestors(target, node_type="job"))
    print("  Upstream Datasets:", pg.get_upstream_ancestors(target, node_type="dataset"))

    broken_source = "job_ingest_customers"
    print(f"\nBlast Radius if '{broken_source}' fails:")
    print("  Downstream Datasets:", pg.get_downstream_impact(broken_source, node_type="dataset"))
    print("  Downstream Jobs:", pg.get_downstream_impact(broken_source, node_type="job"))

    print("\nMermaid Diagram Representation:")
    print(pg.to_mermaid())

