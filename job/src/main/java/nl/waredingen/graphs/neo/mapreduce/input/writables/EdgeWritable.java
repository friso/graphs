package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class EdgeWritable  implements WritableComparable {

	private LongWritable edgeId = new LongWritable();
	private LongWritable fromNodeId = new LongWritable();
	private LongWritable toNodeId = new LongWritable();
	private LongWritable edgePropId = new LongWritable();

	public EdgeWritable() {
		
	}

	public EdgeWritable(long edgeId, long from, long to, long prop) {
		this.edgeId = new LongWritable(edgeId);
		this.fromNodeId = new LongWritable(from);
		this.toNodeId = new LongWritable(to);
		this.edgePropId = new LongWritable(prop);
	}

	public void set(LongWritable edgeId, LongWritable fromNodeId, LongWritable toNodeId, LongWritable prop) {
		this.edgeId = edgeId;
		this.fromNodeId = fromNodeId;
		this.toNodeId = toNodeId;
		this.edgePropId = prop;
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
	
	@Override
	public void write(DataOutput out) throws IOException {
		edgeId.write(out);
		fromNodeId.write(out);
		toNodeId.write(out);
		edgePropId.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		edgeId.readFields(in);		
		fromNodeId.readFields(in);		
		toNodeId.readFields(in);
		edgePropId.readFields(in);
	}
	
	@Override
	public int compareTo(Object obj) {
		EdgeWritable other = (EdgeWritable) obj;
		int edgeDiff = edgeId.compareTo(other.edgeId);
		int fromDiff = fromNodeId.compareTo(other.fromNodeId);
		int toDiff = toNodeId.compareTo(other.toNodeId);
		int propDiff = edgePropId.compareTo(other.edgePropId);
		return (edgeDiff == 0) ? (fromDiff == 0) ? (toDiff ==0) ? propDiff : toDiff: fromDiff : edgeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edgeId == null) ? 0 : edgeId.hashCode());
		result = prime * result + ((fromNodeId == null) ? 0 : fromNodeId.hashCode());
		result = prime * result + ((toNodeId == null) ? 0 : toNodeId.hashCode());
		result = prime * result + ((edgePropId == null) ? 0 : edgePropId.hashCode());
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
		EdgeWritable other = (EdgeWritable) obj;
		if (edgeId == null) {
			if (other.edgeId != null)
				return false;
		} else if (!edgeId.equals(other.edgeId))
			return false;
		if (fromNodeId == null) {
			if (other.fromNodeId != null)
				return false;
		} else if (!fromNodeId.equals(other.fromNodeId))
			return false;
		if (toNodeId == null) {
			if (other.toNodeId != null)
				return false;
		} else if (!toNodeId.equals(other.toNodeId))
			return false;
		if (edgePropId == null) {
			if (other.edgePropId != null)
				return false;
		} else if (!edgePropId.equals(other.edgePropId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EdgeWritable [edgeId=" + edgeId + ", fromNodeId=" + fromNodeId + ", toNodeId=" + toNodeId  + ", edgePropId=" + edgePropId+ "]";
	}

	
}