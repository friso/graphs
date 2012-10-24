package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class NodeEdgeIdWritable implements WritableComparable {

	private LongWritable nodeId = new LongWritable();
	private LongWritable propId = new LongWritable();
	private LongWritable edgeId = new LongWritable();
	
	public NodeEdgeIdWritable() {
		
	}
	
	public NodeEdgeIdWritable(long nodeId, long propId, long edgeId) {
		this.nodeId = new LongWritable(nodeId);
		this.propId = new LongWritable(propId);
		this.edgeId = new LongWritable(edgeId);
	}
	
	public void set(LongWritable nodeId, LongWritable propId, LongWritable edgeId) {
		this.nodeId = nodeId;
		this.propId = propId;
		this.edgeId = edgeId;
	}
	
	public LongWritable getNodeId() {
		return nodeId;
	}

	public LongWritable getPropId() {
		return propId;
	}
	
	public LongWritable getEdgeId() {
		return edgeId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		propId.write(out);
		edgeId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		propId.readFields(in);
		edgeId.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		NodeEdgeIdWritable other = (NodeEdgeIdWritable) obj;
		int nodeDiff = nodeId.compareTo(other.nodeId);
		return (nodeDiff == 0) ? edgeId.compareTo(other.edgeId) : nodeDiff;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edgeId == null) ? 0 : edgeId.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
		result = prime * result + ((propId == null) ? 0 : propId.hashCode());
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
		NodeEdgeIdWritable other = (NodeEdgeIdWritable) obj;
		if (edgeId == null) {
			if (other.edgeId != null)
				return false;
		} else if (!edgeId.equals(other.edgeId))
			return false;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		if (propId == null) {
			if (other.propId != null)
				return false;
		} else if (!propId.equals(other.propId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodeEdgeIdWritable [nodeId=" + nodeId + ", propId=" + propId + ", edgeId=" + edgeId + "]";
	}

}

