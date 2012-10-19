package nl.waredingen.graphs.neo;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputRownumPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class RownumPartitionerTest {
	private Configuration conf = new Configuration();
	private NodeOutputRownumPartitioner<LongWritable, Text> partitioner = new NodeOutputRownumPartitioner<LongWritable, Text>();

	@Before
	public void setup() {
		conf.set(AbstractMetaData.METADATA_NUMBER_OF_NODES, "100");
		partitioner.setConf(conf);
	}
	
	@Test
	public void testPartitioningWith50Reducers() {
		assertThat(partitioner.getPartition(new LongWritable(0), new Text(), 50), is(0));
		assertThat(partitioner.getPartition(new LongWritable(1), new Text(), 50), is(0));
		assertThat(partitioner.getPartition(new LongWritable(2), new Text(), 50), is(1));
		assertThat(partitioner.getPartition(new LongWritable(3), new Text(), 50), is(1));
		assertThat(partitioner.getPartition(new LongWritable(4), new Text(), 50), is(2));
		assertThat(partitioner.getPartition(new LongWritable(5), new Text(), 50), is(2));
		assertThat(partitioner.getPartition(new LongWritable(6), new Text(), 50), is(3));
		assertThat(partitioner.getPartition(new LongWritable(7), new Text(), 50), is(3));
		assertThat(partitioner.getPartition(new LongWritable(8), new Text(), 50), is(4));
		assertThat(partitioner.getPartition(new LongWritable(9), new Text(), 50), is(4));
		assertThat(partitioner.getPartition(new LongWritable(10), new Text(), 50), is(5));
		assertThat(partitioner.getPartition(new LongWritable(11), new Text(), 50), is(5));
		assertThat(partitioner.getPartition(new LongWritable(12), new Text(), 50), is(6));
		assertThat(partitioner.getPartition(new LongWritable(98), new Text(), 50), is(49));
		assertThat(partitioner.getPartition(new LongWritable(99), new Text(), 50), is(49));
	}

	@Test
	public void testPartitioningWith7Reducers() {
		
		assertThat(partitioner.getPartition(new LongWritable(0), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(1), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(2), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(3), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(4), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(5), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(6), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(7), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(8), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(9), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(10), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(11), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(12), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(13), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(14), new Text(), 7), is(0));
		assertThat(partitioner.getPartition(new LongWritable(15), new Text(), 7), is(1));
		assertThat(partitioner.getPartition(new LongWritable(16), new Text(), 7), is(1));
		assertThat(partitioner.getPartition(new LongWritable(29), new Text(), 7), is(2));
		assertThat(partitioner.getPartition(new LongWritable(98), new Text(), 7), is(6));
		assertThat(partitioner.getPartition(new LongWritable(99), new Text(), 7), is(6));
	}

}
