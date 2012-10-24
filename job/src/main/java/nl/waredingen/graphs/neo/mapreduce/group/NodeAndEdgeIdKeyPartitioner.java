package nl.waredingen.graphs.neo.mapreduce.group;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.apache.hadoop.mapreduce.Partitioner;

public class NodeAndEdgeIdKeyPartitioner extends Partitioner<NodeEdgeIdWritable, EdgeWritable> {

	@Override
	public int getPartition(NodeEdgeIdWritable key, EdgeWritable val, int numPartitions) {
		return (key.getNodeId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
