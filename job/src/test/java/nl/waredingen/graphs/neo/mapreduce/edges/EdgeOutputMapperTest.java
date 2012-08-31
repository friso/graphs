package nl.waredingen.graphs.neo.mapreduce.edges;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeOutputMapperTest {
	private MapDriver<LongWritable, Text, LongWritable, Text> driver;
	private List<Pair<LongWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, LongWritable, Text>(new EdgeOutputMapper());
	}

	@Test
	public void shouldOutputAsEdgeWhenNodeIdMatchesFromNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	0	0	1	2	-1	1	0	0	1	1	-1")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(0)));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	1	2	-1	1	-1")));

	}

	@Test
	public void shouldOutputAsEdgeWhereEdgeIdIsTheKeyAndNodeIdMatchesFromNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("2	3	2	0	-1	2	0	3	2	0	-1	2")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(3)));
		assertThat(output.get(0).getSecond(), equalTo(new Text("2	0	-1	2	-1	2")));

	}

	@Test
	public void shouldNotOutputAsEdgeWhenNodeIdDoesNotMatchFromNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	3	2	0	-1	2	2	3	2	0	-1	2")).run();

		assertThat(output.size(), is(0));
	}


}
