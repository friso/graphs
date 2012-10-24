package nl.waredingen.graphs.neo.mapreduce;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AscLongDescLongKeyComparator extends WritableComparator {
	public AscLongDescLongKeyComparator() {
		super(AscLongDescLongWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		AscLongDescLongWritable k1 = (AscLongDescLongWritable) w1;
		AscLongDescLongWritable k2 = (AscLongDescLongWritable) w2;

		int result = k1.getLeft().compareTo(k2.getLeft());
		if (0 == result) {
			result = -1 * k1.getRight().compareTo(k2.getRight());
		}
		return result;
	}

}
