package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class EdgeWritableKeyGroupingComparator extends WritableComparator {
	protected EdgeWritableKeyGroupingComparator() {
		super(EdgeWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		EdgeWritable k1 = (EdgeWritable) w1;
		EdgeWritable k2 = (EdgeWritable) w2;

		return k1.compareTo(k2);
	}

}
