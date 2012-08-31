package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesReducerTest {
	private ReduceDriver<Text, Text, NullWritable, Text> driver;
	private List<Pair<NullWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<Text, Text, NullWritable, Text>(new GroupNodesAndEdgesReducer());
	}

	@Test
	public void shouldOutputAsIs() throws Exception {
		output = driver.withInputKey(new Text("0	A	Aname;0")).withInputValue(new Text("0	0	1")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	Aname	0	0	1")));

	}
}
