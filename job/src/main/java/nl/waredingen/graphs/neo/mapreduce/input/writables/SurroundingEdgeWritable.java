package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class SurroundingEdgeWritable implements WritableComparable {

	private LongWritable nodeId = new LongWritable();
	private LongWritable edgeId = new LongWritable();
	private LongWritable fromNodeId = new LongWritable();
	private LongWritable toNodeId = new LongWritable();
	private LongWritable edgePropId = new LongWritable();
	private LongWritable edgePrev = new LongWritable();
	private LongWritable edgeNext = new LongWritable();

	public SurroundingEdgeWritable() {

	}

	public SurroundingEdgeWritable(long nodeId, long edgeId, long fromNode, long toNode, long edgeProp, long prev, long next) {
		this.set(nodeId, edgeId, fromNode, toNode, edgeProp, prev, next);
	}

	public SurroundingEdgeWritable(SurroundingEdgeWritable other) {
		this.set(other.getNodeId().get(), other.getEdgeId().get(), other.getFromNodeId().get(), other.getToNodeId().get(), other.getEdgePropId().get(), other
				.getEdgePrev().get(), other.getEdgeNext().get());
	}

	public LongWritable getNodeId() {
		return nodeId;
	}

	public LongWritable getEdgeId() {
		return edgeId;
	}

	public LongWritable getFromNodeId() {
		return fromNodeId;
	}

	public LongWritable getToNodeId() {
		return toNodeId;
	}

	public LongWritable getEdgePropId() {
		return edgePropId;
	}
	
	public LongWritable getEdgePrev() {
		return edgePrev;
	}

	public LongWritable getEdgeNext() {
		return edgeNext;
	}

	public void set(long nodeId, long edgeId, long fromNode, long toNode, long edgeProp, long prev, long next) {
		this.nodeId = new LongWritable(nodeId);
		this.edgeId = new LongWritable(edgeId);
		this.fromNodeId = new LongWritable(fromNode);
		this.toNodeId = new LongWritable(toNode);
		this.edgePropId = new LongWritable(edgeProp);
		this.edgePrev = new LongWritable(prev);
		this.edgeNext = new LongWritable(next);
	}

	public void set(LongWritable nodeId, LongWritable edgeId, LongWritable fromNodeId, LongWritable toNodeId, LongWritable edgeProp, LongWritable edgePrev,
			LongWritable edgeNext) {
		this.nodeId = nodeId;
		this.edgeId = edgeId;
		this.fromNodeId = fromNodeId;
		this.toNodeId = toNodeId;
		this.edgePropId = edgeProp;
		this.edgePrev = edgePrev;
		this.edgeNext = edgeNext;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		edgeId.write(out);
		fromNodeId.write(out);
		toNodeId.write(out);
		edgePropId.write(out);
		edgePrev.write(out);
		edgeNext.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		edgeId.readFields(in);
		fromNodeId.readFields(in);
		toNodeId.readFields(in);
		edgePropId.readFields(in);
		edgePrev.readFields(in);
		edgeNext.readFields(in);
	}

	@Override
	public int compareTo(Object obj) {
		SurroundingEdgeWritable other = (SurroundingEdgeWritable) obj;
		int nodeDiff = nodeId.compareTo(other.nodeId);
		int edgeDiff = edgeId.compareTo(other.edgeId);
		int fromDiff = fromNodeId.compareTo(other.fromNodeId);
		int toDiff = toNodeId.compareTo(other.toNodeId);
		int propDiff = edgePropId.compareTo(other.edgePropId);
		int prevDiff = edgePrev.compareTo(other.edgePrev);
		int nextDiff = edgeNext.compareTo(other.edgeNext);
		return (nodeDiff == 0) ? (edgeDiff == 0) ? (fromDiff == 0) ? (toDiff == 0) ? (propDiff == 0) ? (prevDiff == 0) ? nextDiff : prevDiff : propDiff
				: toDiff : fromDiff : edgeDiff : nodeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edgeId == null) ? 0 : edgeId.hashCode());
		result = prime * result + ((edgeNext == null) ? 0 : edgeNext.hashCode());
		result = prime * result + ((edgePrev == null) ? 0 : edgePrev.hashCode());
		result = prime * result + ((edgePropId == null) ? 0 : edgePropId.hashCode());
		result = prime * result + ((fromNodeId == null) ? 0 : fromNodeId.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
		result = prime * result + ((toNodeId == null) ? 0 : toNodeId.hashCode());
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
		SurroundingEdgeWritable other = (SurroundingEdgeWritable) obj;
		if (edgeId == null) {
			if (other.edgeId != null)
				return false;
		} else if (!edgeId.equals(other.edgeId))
			return false;
		if (edgeNext == null) {
			if (other.edgeNext != null)
				return false;
		} else if (!edgeNext.equals(other.edgeNext))
			return false;
		if (edgePrev == null) {
			if (other.edgePrev != null)
				return false;
		} else if (!edgePrev.equals(other.edgePrev))
			return false;
		if (edgePropId == null) {
			if (other.edgePropId != null)
				return false;
		} else if (!edgePropId.equals(other.edgePropId))
			return false;
		if (fromNodeId == null) {
			if (other.fromNodeId != null)
				return false;
		} else if (!fromNodeId.equals(other.fromNodeId))
			return false;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		if (toNodeId == null) {
			if (other.toNodeId != null)
				return false;
		} else if (!toNodeId.equals(other.toNodeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SurroundingEdgeWritable [nodeId=" + nodeId + ", edgeId=" + edgeId + ", fromNodeId=" + fromNodeId + ", toNodeId=" + toNodeId + ", edgePropId="
				+ edgePropId + ", edgePrev=" + edgePrev + ", edgeNext=" + edgeNext + "]";
	}

}