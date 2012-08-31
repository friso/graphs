package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.DualInputMapReduceDriver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinFromNodesAndEdgesMapReduceTest {

	private DualInputMapReduceDriver<LongWritable,Text,LongWritable,Text,Text,Text,Text,Text> driver;
	private List<Pair<Text, Text>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new DualInputMapReduceDriver<LongWritable, Text, LongWritable, Text, Text, Text, Text, Text>();
		driver.setFirstMapper(new JoinNodesMapper());
		driver.setSecondMapper(new JoinFromEdgesMapper());
		driver.setReducer(new JoinNodesAndEdgesReducer());
		driver.setKeyGroupingComparator(new NodeKeyGroupingComparator());
		driver.setKeyOrderComparator(new NodeAndEdgeKeyComparator());
	}

	@Test
	public void shouldjoinFromNodeAndEdge() throws Exception {
		driver.withFirstInput(new LongWritable(0), new Text("0	A	Aname")).addInput(new LongWritable(1), new Text("1	B	Bname"));
		output = driver.withSecondInput(new LongWritable(0), new Text("0	A	B")).run();

		assertThat(output.size(), is(1));
		assertThat(output.get(0).getFirst(), equalTo(new Text("RB")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	B	0	A	Aname")));
	}
}
