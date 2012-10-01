package nl.waredingen.graphs.neo.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NodeAndEdgeIdKeyPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		String keyString = key.toString();
		int hash = keyString.substring(0,keyString.lastIndexOf(";")).hashCode();
		return (hash & Integer.MAX_VALUE) % numPartitions;
	}

}
