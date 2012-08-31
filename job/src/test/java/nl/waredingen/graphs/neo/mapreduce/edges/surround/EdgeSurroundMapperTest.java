package nl.waredingen.graphs.neo.mapreduce.edges.surround;

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

public class EdgeSurroundMapperTest {
	private MapDriver<LongWritable, Text, AscLongDescLongWritable, Text> driver;
	private List<Pair<AscLongDescLongWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, AscLongDescLongWritable, Text>(new EdgeSurroundMapper());
	}

	@Test
	public void shouldOutputAsNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	A	Aname	3	2	0")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0), new LongWritable(3))));
		assertThat(output.get(0).getSecond(), equalTo(new Text("2	0")));

	}

	@Test
	public void shouldOutputAsNodeWhereNodeIdIsTheKey() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("11	A	Aname	3	2	0")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(11), new LongWritable(3))));
		assertThat(output.get(0).getSecond(), equalTo(new Text("2	0")));

	}
}
