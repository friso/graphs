package nl.waredingen.graphs.neo.mapreduce.properties;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgePropertyOutputCountersAndValueWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class EdgePropertyOutputPartitioner extends Partitioner<ByteMarkerIdPropIdWritable, EdgePropertyOutputCountersAndValueWritable> implements Configurable {

	private long max = 0L;
	private Configuration conf;
	
	@Override
	public int getPartition(ByteMarkerIdPropIdWritable key, EdgePropertyOutputCountersAndValueWritable value, int numPartitions) {

		if (key.getMarker().get() == (byte) RowNumberJob.COUNTER_MARKER) {
			return value.getPartition();
		} else {
			return EdgePropertyOutputPartitioner.partitionForValue(value, numPartitions, max);
		}
	}
	
	public static int partitionForValue(EdgePropertyOutputCountersAndValueWritable value, int numPartitions, long maximumIds) {
		double divider = Math.max(1, (double) maximumIds / numPartitions);
		return (int) (value.getId().get() / divider);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		configure();
		
	}

	private void configure() {
		MetaData metaData = Neo4JUtils.getMetaData(conf);
		this.max = metaData.getNumberOfEdges();
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

}
