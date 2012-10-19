package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinNodesMapperTest {
	private MapDriver<NullWritable, BytesWritable, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, BytesWritable, Text, Text>(new JoinNodesMapper());
	}

	@Test
	public void shouldOutputAsNodeIndex() throws Exception {
		Text val = new Text("0	A	1");
		BytesWritable bv = new BytesWritable(val.getBytes());
		output = driver.withInputKey(NullWritable.get()).withInputValue(bv).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new Text("NA")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	1")));

	}
}
