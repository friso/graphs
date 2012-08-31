package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

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

public class JoinSurroundingEdgesMapperTest {
	private MapDriver<LongWritable, Text, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, Text, Text>(new JoinSurroundingEdgesMapper());
	}

	@Test
	public void shouldOutputAsJoin() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	3	2	0	-1	2")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new Text("3;2;0")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	3	2	0	-1	2")));
	}
}
