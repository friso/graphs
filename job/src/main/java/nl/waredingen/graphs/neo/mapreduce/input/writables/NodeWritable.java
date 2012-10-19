package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class NodeWritable implements WritableComparable {

	private LongWritable nodeId = new LongWritable();
	private LongWritable propId = new LongWritable();
	
	public NodeWritable() {
		
	}
	
	public NodeWritable(long nodeId, long propId) {
		this.set(nodeId, propId);
	}
	
	public void set(long nodeId, long propId) {
		this.nodeId = new LongWritable(nodeId);
		this.propId = new LongWritable(propId);
	}
	
	public void set(LongWritable nodeId, LongWritable propId) {
		this.nodeId = nodeId;
		this.propId = propId;
	}
	
	public LongWritable getNodeId() {
		return nodeId;
	}

	public LongWritable getPropId() {
		return propId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		propId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		propId.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		NodeWritable other = (NodeWritable) obj;
		int nodeDiff = nodeId.compareTo(other.nodeId);
		return (nodeDiff == 0) ? propId.compareTo(other.propId) : nodeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((propId == null) ? 0 : propId.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
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
		NodeWritable other = (NodeWritable) obj;
		if (propId == null) {
			if (other.propId != null)
				return false;
		} else if (!propId.equals(other.propId))
			return false;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodeIdPropIdWritable [nodeId=" + nodeId + ", propId=" + propId + "]";
	}

}

