package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesMapReduceTest {
	private MapReduceDriver<Text, Text, Text, Text, NullWritable, Text> driver;
	private List<Pair<NullWritable, Text>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new MapReduceDriver<Text, Text, Text, Text, NullWritable, Text>(new GroupNodesAndEdgesMapper(), new GroupNodesAndEdgesReducer());
		driver.setKeyGroupingComparator(new NodeAndEdgeIdKeyGroupingComparator());
		driver.setKeyOrderComparator(new NodeAndEdgeIdKeyComparator());
	}

	@Test
	public void shouldOutputWithFromAndToNode() throws Exception {
		output = driver.withInput(new Text("RB"), new Text("0	A	B	0	A	Aname	1	B	Bname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	Aname	0	0	1")));

		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new Text("1	B	Bname	0	0	1")));

	}

}
