package nl.waredingen.graphs.neo.mapreduce;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AscLongDescLongKeyGroupingComparator extends WritableComparator {
	public AscLongDescLongKeyGroupingComparator() {
		super(AscLongDescLongWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		AscLongDescLongWritable k1 = (AscLongDescLongWritable) w1;
		AscLongDescLongWritable k2 = (AscLongDescLongWritable) w2;

		return k1.getLeft().compareTo(k2.getLeft());
	}

}
