package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgePropertyOutputCountersAndValueWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgePropertiesWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgePropertiesOutputMapperTest {

	private MapDriver<LongWritable, FullEdgePropertiesWritable, ByteMarkerIdPropIdWritable, EdgePropertyOutputCountersAndValueWritable> driver;
	private List<Pair<ByteMarkerIdPropIdWritable, EdgePropertyOutputCountersAndValueWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, FullEdgePropertiesWritable, ByteMarkerIdPropIdWritable, EdgePropertyOutputCountersAndValueWritable>(new EdgePropertyOutputMapper());
		Configuration configuration = new Configuration();
		configuration.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 1);
		configuration.setInt("mapred.reduce.tasks", 3);
		driver.setConfiguration(configuration );
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		FullEdgePropertiesWritable edge = new FullEdgePropertiesWritable(0, "A", "B", 4, 0, -1, 1, 0, "AProp");
		edge.add(1, "BProp", 0);
		output = driver.withInputKey(new LongWritable(0)).withInputValue(edge).run();

		assertThat(output.size(), is(3));

		EdgePropertyOutputCountersAndValueWritable val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(new LongWritable(0), edge);
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(0), new IntWritable(4))));
		assertThat(output.get(0).getSecond(), equalTo(val));

		val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(EdgePropertyOutputCountersAndValueWritable.EMPTY_ID, EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(1, 0, 1);
		
		assertThat(output.get(1).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(1).getSecond(), equalTo(val));

		val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(EdgePropertyOutputCountersAndValueWritable.EMPTY_ID, EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(2, 0, 1);
		
		assertThat(output.get(2).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(2).getSecond(), equalTo(val));

	}
	
	@Test
	public void shouldOutputAsPropertieswithPropertyIdAsTheKey() throws Exception {
		output = driver.withInputKey(new LongWritable(1)).withInputValue(new FullEdgePropertiesWritable(1, "A", "B", 2, 40, 1, 2, 4, "AProp")).run();

		assertThat(output.size(), is(3));

		EdgePropertyOutputCountersAndValueWritable val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(new LongWritable(1), new FullEdgePropertiesWritable(1, "A", "B", 2, 40, 1, 2, 4, "AProp"));
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(1), new IntWritable(2))));
		assertThat(output.get(0).getSecond(), equalTo(val));
		
		val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(EdgePropertyOutputCountersAndValueWritable.EMPTY_ID, EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(1, 0, 0);
		
		assertThat(output.get(1).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(1).getSecond(), equalTo(val));
		
		val = new EdgePropertyOutputCountersAndValueWritable();
		val.setValues(EdgePropertyOutputCountersAndValueWritable.EMPTY_ID, EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL);
		val.setCounter(2, 40, 1);
		
		assertThat(output.get(2).getFirst(), equalTo(new ByteMarkerIdPropIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE), new IntWritable(Integer.MIN_VALUE))));
		assertThat(output.get(2).getSecond(), equalTo(val));
		
	}
}
