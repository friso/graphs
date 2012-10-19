package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class NodeEdgeWritable  implements WritableComparable {

	private NodeWritable node = new NodeWritable();
	private EdgeWritable edge = new EdgeWritable();

	public NodeEdgeWritable() {

	}
	
	public NodeEdgeWritable(long nodeId, long nodePropId, long edgeId, long from, long to, long edgePropId) {
		this.set(nodeId, nodePropId, edgeId, from, to, edgePropId);
	}

	public NodeWritable getNode() {
		return node;
	}

	public EdgeWritable getEdge() {
		return edge;
	}
	
	public void set(long nodeId, long nodePropId, long edgeId, long from, long to, long edgePropId) {
		this.node = new NodeWritable(nodeId, nodePropId);
		this.edge = new EdgeWritable(edgeId, from , to, edgePropId);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		node.write(out);
		edge.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		node.readFields(in);		
		edge.readFields(in);		
	}
	
	@Override
	public int compareTo(Object obj) {
		NodeEdgeWritable other = (NodeEdgeWritable) obj;
		int nodeDiff = node.compareTo(other.node);
		int edgeDiff = edge.compareTo(other.edge);
		return (nodeDiff == 0) ? edgeDiff : nodeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edge == null) ? 0 : edge.hashCode());
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeEdgeWritable other = (NodeEdgeWritable) obj;
		if (edge == null) {
			if (other.edge != null)
				return false;
		} else if (!edge.equals(other.edge))
			return false;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodeEdgeWritable [node=" + node + ", edge=" + edge + "]";
	}

}