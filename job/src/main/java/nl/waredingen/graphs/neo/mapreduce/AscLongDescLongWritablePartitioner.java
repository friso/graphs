package nl.waredingen.graphs.neo.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AscLongDescLongWritablePartitioner extends Partitioner<AscLongDescLongWritable, Text> {

	@Override
	public int getPartition(AscLongDescLongWritable key, Text value, int numPartitions) {
		return key.getLeft().hashCode() % numPartitions;
	}

}
