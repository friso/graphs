package nl.waredingen.graphs.neo.mapreduce;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

@SuppressWarnings("rawtypes")
public class AscLongDescLongWritablePartitioner extends Partitioner<AscLongDescLongWritable, WritableComparable> {

	@Override
	public int getPartition(AscLongDescLongWritable key, WritableComparable value, int numPartitions) {
		return key.getLeft().hashCode() % numPartitions;
	}

}
