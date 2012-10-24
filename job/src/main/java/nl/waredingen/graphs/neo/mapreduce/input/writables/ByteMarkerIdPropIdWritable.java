package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class ByteMarkerIdPropIdWritable implements WritableComparable {

	private ByteWritable marker = new ByteWritable();
	private LongWritable id = new LongWritable();
	private IntWritable propId = new IntWritable();
	
	public ByteMarkerIdPropIdWritable() {
		
	}
	
	public ByteMarkerIdPropIdWritable(ByteWritable marker, LongWritable id, IntWritable propId) {
		this.marker = marker;
		this.id = id;
		this.propId = propId;
	}
	
	public void setMarker(ByteWritable marker) {
		this.marker = marker;
	}
	
	public void setIds(LongWritable id, IntWritable propId) {
		this.id = id;
		this.propId = propId;
	}
	
	public ByteWritable getMarker() {
		return marker;
	}
	
	public LongWritable getId() {
		return id;
	}
	
	public IntWritable getPropId() {
		return propId;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		marker.write(out);
		id.write(out);
		propId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		marker.readFields(in);
		id.readFields(in);		
		propId.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		ByteMarkerIdPropIdWritable other = (ByteMarkerIdPropIdWritable) obj;
		int markerDiff = marker.compareTo(other.marker);
		int nodeDiff = id.compareTo(other.id);
		// sort on marker and then on nodeId and propertyId
		return (markerDiff == 0) ?  (nodeDiff == 0) ? propId.compareTo(other.propId) : nodeDiff: markerDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((propId == null) ? 0 : propId.hashCode());
		result = prime * result + ((marker == null) ? 0 : marker.hashCode());
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
		ByteMarkerIdPropIdWritable other = (ByteMarkerIdPropIdWritable) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (propId == null) {
			if (other.propId != null)
				return false;
		} else if (!propId.equals(other.propId))
			return false;
		if (marker == null) {
			if (other.marker != null)
				return false;
		} else if (!marker.equals(other.marker))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ByteMarkerIdPropIdWritable [marker=" + marker + ", id=" + id +  ", propId=" + propId + "]";
	}

}