package nl.waredingen.graphs.neo.mapreduce.properties;

import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ByteMarkerAndIdComparator extends WritableComparator {
	protected ByteMarkerAndIdComparator() {
		super(ByteMarkerIdPropIdWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ByteMarkerIdPropIdWritable k1 = (ByteMarkerIdPropIdWritable) w1;
		ByteMarkerIdPropIdWritable k2 = (ByteMarkerIdPropIdWritable) w2;

		int result = k1.getMarker().compareTo(k2.getMarker());
		if (0 == result) {
			result = k1.getId().compareTo(k2.getId());
			if (0 == result) {
				result = k1.getPropId().compareTo(k2.getPropId());
			}
		}
		return result;
	}

}
