package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodePropertyOutputCountersAndValueWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodePropertiesOutputMapperTest {

	private MapDriver<LongWritable, FullNodePropertiesWritable, ByteMarkerIdPropIdWritable, NodePropertyOutputCountersAndValueWritable> driver;
	private List<Pair<ByteMarkerIdPropIdWritable, NodePropertyOutputCountersAndValueWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, FullNodePropertiesWritable, ByteMarkerIdPropIdWritable, NodePropertyOutputCountersAndValueWritable>(new NodePropertyOutputMapper());
		Configuration configuration = new Configuration();
		configuration.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 1);
		configuration.setInt("mapred.reduce.tasks", 3);
		driver.setConfiguration(configuration );
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		FullNodePropertiesWritable node = new FullNodePropertiesWritable(0, "A", 0, 0, -1, 1, 0,"A");
		node.add(1, "Aname", 0);
		output = driver.withInputKey(new LongWritable(0)).withInputValue(node).run();

		assertThat(output.size(), is(3));

		NodePropertyOutputCountersAndValueWritable val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(new LongWritable(0), node);
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(0), new IntWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(val));

		val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(NodePropertyOutputCountersAndValueWritable.EMPTY_ID, NodePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(1, 0, 1);
		
		assertThat(output.get(1).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(1).getSecond(), equalTo(val));

		val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(NodePropertyOutputCountersAndValueWritable.EMPTY_ID, NodePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(2, 0, 1);
		
		assertThat(output.get(2).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(2).getSecond(), equalTo(val));

	}
	
	@Test
	public void shouldOutputAsPropertieswithPropertyIdAsTheKey() throws Exception {
		output = driver.withInputKey(new LongWritable(1)).withInputValue(new FullNodePropertiesWritable(1, "A", 1, 40, 1, 2, 1, "Aname")).run();

		assertThat(output.size(), is(3));

		NodePropertyOutputCountersAndValueWritable val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(new LongWritable(1), new FullNodePropertiesWritable(1, "A", 1, 40, 1, 2, 1, "Aname"));
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(1), new IntWritable(1))));
		assertThat(output.get(0).getSecond(), equalTo(val));
		
		val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(NodePropertyOutputCountersAndValueWritable.EMPTY_ID, NodePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(1, 0, 0);
		
		assertThat(output.get(1).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(1).getSecond(), equalTo(val));
		
		val = new NodePropertyOutputCountersAndValueWritable();
		val.setValues(NodePropertyOutputCountersAndValueWritable.EMPTY_ID, NodePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(2, 40, 1);
		
		assertThat(output.get(2).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(2).getSecond(), equalTo(val));
		
	}
}
