package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.PureMRNodesAndEdgesJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PropertiesOutputMapperTest {

	private MapDriver<LongWritable, Text, ByteMarkerPropertyIdWritable, PropertyOutputIdBlockcountValueWritable> driver;
	private List<Pair<ByteMarkerPropertyIdWritable, PropertyOutputIdBlockcountValueWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, ByteMarkerPropertyIdWritable, PropertyOutputIdBlockcountValueWritable>(new PropertyOutputMapper());
		Configuration configuration = new Configuration();
		configuration.setLong(PureMRNodesAndEdgesJob.NUMBEROFROWS_CONFIG, 1);
		configuration.setInt("mapred.reduce.tasks", 3);
		driver.setConfiguration(configuration );
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	0	0	A	0	-1	1")).run();

		assertThat(output.size(), is(1));

		PropertyOutputIdBlockcountValueWritable val = new PropertyOutputIdBlockcountValueWritable();
		val.setValues(new LongWritable(0), new Text("0	A	0	-1	1"));
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerPropertyIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(val));
		
	}
	
	@Test
	public void shouldOutputAsPropertieswithPropertyIdAsTheKey() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	1	1	Aname	40	1	2")).run();

		assertThat(output.size(), is(2));

		PropertyOutputIdBlockcountValueWritable val = new PropertyOutputIdBlockcountValueWritable();
		val.setValues(new LongWritable(1), new Text("1	Aname	40	1	2"));
		
		assertThat(output.get(0).getFirst(), equalTo(new ByteMarkerPropertyIdWritable(new ByteWritable(RowNumberJob.VALUE_MARKER), new LongWritable(1))));
		assertThat(output.get(0).getSecond(), equalTo(val));
		
		val = new PropertyOutputIdBlockcountValueWritable();
		val.setValues(PropertyOutputIdBlockcountValueWritable.EMPTY_ID, PropertyOutputIdBlockcountValueWritable.EMPTY_STRING);
		val.setCounter(2, 40);
		
		assertThat(output.get(1).getFirst(), equalTo(new ByteMarkerPropertyIdWritable(new ByteWritable(RowNumberJob.COUNTER_MARKER), new LongWritable(Long.MIN_VALUE))));
		assertThat(output.get(1).getSecond(), equalTo(val));
		
	}
}
