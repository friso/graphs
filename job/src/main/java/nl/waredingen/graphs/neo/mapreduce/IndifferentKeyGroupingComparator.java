package nl.waredingen.graphs.neo.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndifferentKeyGroupingComparator extends WritableComparator {

	protected IndifferentKeyGroupingComparator() {
		super(LongWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return 0;
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return 0;
	}
}
