package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.join.JoinFromEdgesMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinFromEdgesMapperTest {
	private MapDriver<LongWritable, Text, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, Text, Text>(new JoinFromEdgesMapper());
	}

	@Test
	public void shouldOutputAsFromNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	A	B")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new Text("EA")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	B")));

	}
}
