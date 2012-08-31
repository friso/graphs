package nl.waredingen.graphs.neo.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeAndEdgeIdKeyGroupingComparator extends WritableComparator {
	protected NodeAndEdgeIdKeyGroupingComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		String key1 = k1.toString().split(";")[0];
		String key2 = k2.toString().split(";")[0];
		return key1.compareTo(key2);
	}

}
