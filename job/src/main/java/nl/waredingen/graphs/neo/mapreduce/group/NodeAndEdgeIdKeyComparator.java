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

		String k1s = k1.toString();
		String key1 = k1s.substring(0, k1s.lastIndexOf(";"));
		Long edgeId = Long.valueOf(k1s.substring(k1s.lastIndexOf(";")+1));
		String k2s = k2.toString();
		String key2 = k2s.substring(0, k2s.lastIndexOf(";"));
		Long edgeId2 = Long.valueOf(k2s.substring(k2s.lastIndexOf(";")+1));
		
		int result = key1.compareTo(key2);
		if (0 == result) {
			result = edgeId.compareTo(edgeId2);
		}
		return result;
	}

}
