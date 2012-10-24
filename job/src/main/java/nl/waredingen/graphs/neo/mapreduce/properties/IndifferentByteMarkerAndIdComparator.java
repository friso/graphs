package nl.waredingen.graphs.neo.mapreduce.properties;

import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;

import org.apache.hadoop.io.RawComparator;

public class IndifferentByteMarkerAndIdComparator implements RawComparator<ByteMarkerIdPropIdWritable> {
	@Override
	public int compare(ByteMarkerIdPropIdWritable left, ByteMarkerIdPropIdWritable right) {
		return 0;
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return 0;
	}
}