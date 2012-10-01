package nl.waredingen.graphs.neo.mapreduce.properties;

import org.apache.hadoop.io.RawComparator;

public class IndifferentByteMarkerAndPropertyOutputIdComparator implements RawComparator<ByteMarkerPropertyIdWritable> {
	@Override
	public int compare(ByteMarkerPropertyIdWritable left, ByteMarkerPropertyIdWritable right) {
		return 0;
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return 0;
	}
}