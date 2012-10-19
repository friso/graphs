package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinToEdgesMapperTest {
	private MapDriver<NullWritable, Text, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, Text, Text, Text>(new JoinToEdgesMapper());
	}

	@Test
	public void shouldOutputAsToNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("0	A	B	0	1")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new Text("EB")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	B	0	1")));

	}
}
