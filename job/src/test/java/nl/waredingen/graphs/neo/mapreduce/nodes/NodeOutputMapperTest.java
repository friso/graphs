package nl.waredingen.graphs.neo.mapreduce.nodes;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodeOutputMapperTest {
	private MapDriver<LongWritable, Text, LongWritable, Text> driver;
	private List<Pair<LongWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, LongWritable, Text>(new NodeOutputMapper());
	}

	@Test
	public void shouldOutputAsNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	A	Aname	3	2	0")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(0)));
		assertThat(output.get(0).getSecond(), equalTo(new Text("3	0")));

	}

	@Test
	public void shouldOutputAsNodeWhereNodeIdIsTheKey() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("11	A	Aname	3	2	0")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(11)));
		assertThat(output.get(0).getSecond(), equalTo(new Text("3	22")));

	}
}
