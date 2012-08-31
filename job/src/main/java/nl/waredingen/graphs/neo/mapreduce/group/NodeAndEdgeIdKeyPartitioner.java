package nl.waredingen.graphs.neo.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NodeAndEdgeIdKeyPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		int hash = key.toString().split(";")[0].hashCode();
		return hash % numPartitions;
	}

}
