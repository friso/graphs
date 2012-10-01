package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class ByteMarkerPropertyIdWritable implements WritableComparable {

	private ByteWritable marker = new ByteWritable();
	private LongWritable id = new LongWritable();
	
	public ByteMarkerPropertyIdWritable() {
		
	}
	
	public ByteMarkerPropertyIdWritable(ByteWritable marker, LongWritable id) {
		this.marker = marker;
		this.id = id;
	}
	
	public void setMarker(ByteWritable marker) {
		this.marker = marker;
	}
	
	public void setId(LongWritable id) {
		this.id = id;
	}
	
	public ByteWritable getMarker() {
		return marker;
	}
	
	public LongWritable getId() {
		return id;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		marker.write(out);
		id.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		marker.readFields(in);
		id.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		ByteMarkerPropertyIdWritable other = (ByteMarkerPropertyIdWritable) obj;
		int markerDiff = marker.compareTo(other.marker);
		// sort on marker and then on id
		return (markerDiff == 0) ? id.compareTo(other.id) : markerDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		ByteMarkerPropertyIdWritable other = (ByteMarkerPropertyIdWritable) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
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
		return marker + "\t" + id;
	}
}