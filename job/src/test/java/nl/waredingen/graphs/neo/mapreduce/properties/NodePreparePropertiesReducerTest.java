package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodePreparePropertiesReducerTest {
	private ReduceDriver<AscLongDescLongWritable, FullNodePropertiesWritable, LongWritable, FullNodePropertiesWritable> driver;
	private List<Pair<LongWritable, FullNodePropertiesWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<AscLongDescLongWritable, FullNodePropertiesWritable, LongWritable, FullNodePropertiesWritable>(new NodePreparePropertiesReducer());
	}

	@Test
	public void shouldOutputSurroundingProperties() throws Exception {
		List<FullNodePropertiesWritable> values = new ArrayList<FullNodePropertiesWritable>();
		values.add(new FullNodePropertiesWritable(1, "A", 2, 2, -1, -1, 0, "longblahorsomething"));
		values.add(new FullNodePropertiesWritable(1, "A", 1, 0, -1, -1, 1, "otherblah"));
		values.add(new FullNodePropertiesWritable(1, "A", 0, 0, -1, -1, 2, "blah"));
		//Unfortunately no multiple keys can be added to this reducer, as is in real life.
		output = driver.withInputKey(new AscLongDescLongWritable(new LongWritable(1), new LongWritable(3))).withInputValues(values).run();

		assertThat(output.size(), is(3));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(1)));
		assertThat(output.get(0).getSecond(), equalTo(new FullNodePropertiesWritable(1, "A", 2, 2, 1, -1, 0, "longblahorsomething")));
		assertThat(output.get(1).getFirst(), equalTo(new LongWritable(1)));
		assertThat(output.get(1).getSecond(), equalTo(new FullNodePropertiesWritable(1, "A", 1, 0, 0, 2, 1, "otherblah")));
		assertThat(output.get(2).getFirst(), equalTo(new LongWritable(1)));
		assertThat(output.get(2).getSecond(), equalTo(new FullNodePropertiesWritable(1, "A", 0, 0, -1, 1, 2, "blah")));

	}
	
}
