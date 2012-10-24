package nl.waredingen.graphs.neo.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeKeyGroupingComparator extends WritableComparator {
	protected NodeKeyGroupingComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		return k1.toString().substring(1).compareTo(k2.toString().substring(1));
	}

}
