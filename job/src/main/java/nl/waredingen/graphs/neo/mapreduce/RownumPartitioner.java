package nl.waredingen.graphs.neo.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RownumPartitioner<K, V> extends Partitioner<K, V> implements Configurable {

	private long max = 0L;
	private Configuration conf;
	
	@Override
	public int getPartition(K key, V value, int numPartitions) {
		double divider = Math.max(1, (double) max / numPartitions);

		return (int) (((LongWritable) key).get() / divider);
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
