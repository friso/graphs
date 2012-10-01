package nl.waredingen.graphs.neo.mapreduce.properties;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ByteMarkerAndPropertyOutputIdComparator extends WritableComparator {
	protected ByteMarkerAndPropertyOutputIdComparator() {
		super(ByteMarkerPropertyIdWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ByteMarkerPropertyIdWritable k1 = (ByteMarkerPropertyIdWritable) w1;
		ByteMarkerPropertyIdWritable k2 = (ByteMarkerPropertyIdWritable) w2;

		int result = k1.getMarker().compareTo(k2.getMarker());
		if (0 == result) {
			result = k1.getId().compareTo(k2.getId());
		}
		return result;
	}

}
