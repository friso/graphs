package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class FullEdgeWritable  implements WritableComparable {

	private LongWritable fromNodeId = new LongWritable();
	private LongWritable toNodeId = new LongWritable();
	private LongWritable fromPrev = new LongWritable();
	private LongWritable fromNext = new LongWritable();
	private LongWritable toPrev = new LongWritable();
	private LongWritable toNext = new LongWritable();
	private LongWritable edgeProp = new LongWritable();

	public FullEdgeWritable() {
		
	}
	
	public FullEdgeWritable(long fromNode, long toNode, long edgeProp, long fromPrev, long fromNext, long toPrev, long toNext) {
		this.set(fromNode, toNode, edgeProp, fromPrev, fromNext, toPrev, toNext);
	}
	
	public LongWritable getFromNodeId() {
		return fromNodeId;
	}

	public LongWritable getToNodeId() {
		return toNodeId;
	}

	public LongWritable getEdgeProp() {
		return edgeProp;
	}
	
	public LongWritable getFromPrev() {
		return fromPrev;
	}

	public LongWritable getFromNext() {
		return fromNext;
	}

	public LongWritable getToPrev() {
		return toPrev;
	}

	public LongWritable getToNext() {
		return toNext;
	}

	public void set(long fromNode, long toNode, long edgeProp, long fromPrev, long fromNext, long toPrev, long toNext) {
		this.fromNodeId = new LongWritable(fromNode);
		this.toNodeId = new LongWritable(toNode);
		this.edgeProp = new LongWritable(edgeProp);
		this.fromPrev = new LongWritable(fromPrev);
		this.fromNext = new LongWritable(fromNext);
		this.toPrev = new LongWritable(toPrev);
		this.toNext = new LongWritable(toNext);
	}
	
	public void set(LongWritable fromNode, LongWritable toNode, LongWritable edgeProp, LongWritable fromPrev, LongWritable fromNext, LongWritable toPrev, LongWritable toNext) {
		this.fromNodeId = fromNode;
		this.toNodeId = toNode;
		this.edgeProp = edgeProp;
		this.fromPrev = fromPrev;
		this.fromNext = fromNext;
		this.toPrev = toPrev;
		this.toNext = toNext;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		fromNodeId.write(out);
		toNodeId.write(out);
		edgeProp.write(out);
		fromPrev.write(out);
		fromNext.write(out);
		toPrev.write(out);
		toNext.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		fromNodeId.readFields(in);
		toNodeId.readFields(in);
		edgeProp.readFields(in);
		fromPrev.readFields(in);
		fromNext.readFields(in);
		toPrev.readFields(in);
		toNext.readFields(in);
	}
	
	@Override
	public int compareTo(Object obj) {
		FullEdgeWritable other = (FullEdgeWritable) obj;
		int nodeDiff = fromPrev.compareTo(other.fromPrev);
		int edgeDiff = fromNext.compareTo(other.fromNext);
		int propDiff = edgeProp.compareTo(other.edgeProp);
		int fromDiff = fromNodeId.compareTo(other.fromNodeId);
		int toDiff = toNodeId.compareTo(other.toNodeId);
		int prevDiff = toPrev.compareTo(other.toPrev);
		int nextDiff = toNext.compareTo(other.toNext);
		return (nodeDiff == 0) ? (edgeDiff == 0) ? (propDiff == 0) ? (fromDiff == 0) ? (toDiff == 0) ? (prevDiff == 0) ? nextDiff: prevDiff : toDiff : fromDiff : propDiff : edgeDiff : nodeDiff;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fromNext == null) ? 0 : fromNext.hashCode());
		result = prime * result + ((toNext == null) ? 0 : toNext.hashCode());
		result = prime * result + ((toPrev == null) ? 0 : toPrev.hashCode());
		result = prime * result + ((fromNodeId == null) ? 0 : fromNodeId.hashCode());
		result = prime * result + ((fromPrev == null) ? 0 : fromPrev.hashCode());
		result = prime * result + ((toNodeId == null) ? 0 : toNodeId.hashCode());
		result = prime * result + ((edgeProp == null) ? 0 : edgeProp.hashCode());
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
		FullEdgeWritable other = (FullEdgeWritable) obj;
		if (fromNext == null) {
			if (other.fromNext != null)
				return false;
		} else if (!fromNext.equals(other.fromNext))
			return false;
		if (toNext == null) {
			if (other.toNext != null)
				return false;
		} else if (!toNext.equals(other.toNext))
			return false;
		if (toPrev == null) {
			if (other.toPrev != null)
				return false;
		} else if (!toPrev.equals(other.toPrev))
			return false;
		if (fromNodeId == null) {
			if (other.fromNodeId != null)
				return false;
		} else if (!fromNodeId.equals(other.fromNodeId))
			return false;
		if (fromPrev == null) {
			if (other.fromPrev != null)
				return false;
		} else if (!fromPrev.equals(other.fromPrev))
			return false;
		if (toNodeId == null) {
			if (other.toNodeId != null)
				return false;
		} else if (!toNodeId.equals(other.toNodeId))
			return false;
		if (edgeProp == null) {
			if (other.edgeProp != null)
				return false;
		} else if (!edgeProp.equals(other.edgeProp))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FullEdgeWritable [fromNodeId=" + fromNodeId + ", toNodeId=" + toNodeId + ", edgeProp=" + edgeProp + ", fromPrev=" + fromPrev + ", fromNext=" + fromNext + ", toPrev="
				+ toPrev + ", toNext=" + toNext + "]";
	}


}