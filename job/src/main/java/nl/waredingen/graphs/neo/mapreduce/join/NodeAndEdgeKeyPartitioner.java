package nl.waredingen.graphs.neo.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NodeAndEdgeKeyPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		int hash = key.toString().substring(1).hashCode();
		return hash % numPartitions;
	}

}
