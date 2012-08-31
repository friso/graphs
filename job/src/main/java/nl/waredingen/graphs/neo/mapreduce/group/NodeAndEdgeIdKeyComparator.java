package nl.waredingen.graphs.neo.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeAndEdgeIdKeyComparator extends WritableComparator {
	protected NodeAndEdgeIdKeyComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		String[] keys1 = k1.toString().split(";");
		Long edgeId = Long.valueOf(keys1[1]);
		String[] keys2 = k2.toString().split(";");
		Long edgeId2 = Long.valueOf(keys2[1]);
		
		int result = keys1[0].compareTo(keys2[0]);
		if (0 == result) {
			result = -1 * edgeId.compareTo(edgeId2);
		}
		return result;
	}

}
