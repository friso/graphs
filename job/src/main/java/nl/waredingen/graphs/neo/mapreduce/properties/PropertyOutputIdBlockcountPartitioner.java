package nl.waredingen.graphs.neo.mapreduce.properties;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.PureMRNodesAndEdgesJob;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PropertyOutputIdBlockcountPartitioner extends Partitioner<ByteWritable, PropertyOutputIdBlockcountValueWritable> implements Configurable {

	private long max = 0L;
	private Configuration conf;
	
	@Override
	public int getPartition(ByteWritable key, PropertyOutputIdBlockcountValueWritable value, int numPartitions) {

		if (key.get() == (byte) RowNumberJob.COUNTER_MARKER) {
			return value.getPartition();
		} else {
			return PropertyOutputIdBlockcountPartitioner.partitionForValue(value, numPartitions, max);
		}
	}
	
	public static int partitionForValue(PropertyOutputIdBlockcountValueWritable value, int numPartitions, long maximumIds) {
		double divider = Math.max(1, (double) maximumIds / numPartitions);
		return (int) (value.getId().get() / divider);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		configure();
		
	}

	private void configure() {
		this.max = Long.parseLong(getConf().get(PureMRNodesAndEdgesJob.NUMBEROFROWS_CONFIG));
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

}
