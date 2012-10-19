package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class EdgeIdPropIdWritable implements WritableComparable {

	private LongWritable edgeId = new LongWritable();
	private LongWritable propId = new LongWritable();
	
	public EdgeIdPropIdWritable() {
		
	}
	
	public EdgeIdPropIdWritable(long edgeId, long propId) {
		this.edgeId = new LongWritable(edgeId);
		this.propId = new LongWritable(propId);
	}
	
	public void set(LongWritable edgeId, LongWritable propId) {
		this.edgeId = edgeId;
		this.propId = propId;
	}
	
	public LongWritable getEdgeId() {
		return edgeId;
	}

	public LongWritable getPropId() {
		return propId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		edgeId.write(out);
		propId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		edgeId.readFields(in);
		propId.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		EdgeIdPropIdWritable other = (EdgeIdPropIdWritable) obj;
		int edgeDiff = edgeId.compareTo(other.edgeId);
		return (edgeDiff == 0) ? propId.compareTo(other.edgeId) : edgeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((propId == null) ? 0 : propId.hashCode());
		result = prime * result + ((edgeId == null) ? 0 : edgeId.hashCode());
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
		EdgeIdPropIdWritable other = (EdgeIdPropIdWritable) obj;
		if (propId == null) {
			if (other.propId != null)
				return false;
		} else if (!propId.equals(other.propId))
			return false;
		if (edgeId == null) {
			if (other.edgeId != null)
				return false;
		} else if (!edgeId.equals(other.edgeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EdgeIdPropIdWritable [edgeId=" + edgeId + ", propId=" + propId + "]";
	}

}

