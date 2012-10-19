package nl.waredingen.graphs.neo.mapreduce.group;

import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeAndEdgeIdKeyGroupingComparator extends WritableComparator {
	protected NodeAndEdgeIdKeyGroupingComparator() {
		super(NodeEdgeIdWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		NodeEdgeIdWritable k1 = (NodeEdgeIdWritable) w1;
		NodeEdgeIdWritable k2 = (NodeEdgeIdWritable) w2;

		return k1.getNodeId().compareTo(k2.getNodeId());
	}

}
