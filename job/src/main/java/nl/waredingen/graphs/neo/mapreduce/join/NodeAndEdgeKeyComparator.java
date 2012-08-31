package nl.waredingen.graphs.neo.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeAndEdgeKeyComparator extends WritableComparator {
	protected NodeAndEdgeKeyComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		String identifier1 = k1.toString().substring(0,1);
		String identifier2 = k2.toString().substring(0,1);
		
		int result = k1.toString().substring(1).compareTo(k2.toString().substring(1));
		if (0 == result) {
			result = -1 * identifier1.compareTo(identifier2);
		}
		return result;
	}

}
