package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.join.JoinFromEdgesMapper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class NodePreparePropertiesReducerTest {
	private ReduceDriver<AscLongDescLongWritable, Text, NullWritable, Text> driver;
	private List<Pair<NullWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<AscLongDescLongWritable, Text, NullWritable, Text>(new NodePreparePropertiesReducer());
	}

	@Test
	public void shouldOutputSurroundingProperties() throws Exception {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("blah	0"));
		values.add(new Text("otherblah	0"));
		values.add(new Text("longblahorsomething	2"));
		//Unfortunately no multiple keys can be added to this reducer, as is in real life.
		//This results in an incorrect relnum in this test
		output = driver.withInputKey(new AscLongDescLongWritable(new LongWritable(1), new LongWritable(3))).withInputValues(values).run();

		assertThat(output.size(), is(3));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new Text("1	3	blah	0	-1	3")));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new Text("1	3	otherblah	0	3	3")));
		assertThat(output.get(2).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(2).getSecond(), equalTo(new Text("1	3	longblahorsomething	2	3	-1")));

	}
	
}
