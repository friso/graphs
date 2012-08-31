package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.DualInputMapReduceDriver;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesAndEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.JoinToEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.join.NodeKeyGroupingComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinToNodesAndEdgesMapReduceTest {

	private DualInputMapReduceDriver<LongWritable,Text,Text,Text,Text,Text,Text,Text> driver;
	private List<Pair<Text, Text>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new DualInputMapReduceDriver<LongWritable, Text, Text, Text, Text, Text, Text, Text>();
		driver.setFirstMapper(new JoinNodesMapper());
		driver.setSecondMapper(new JoinToEdgesMapper());
		driver.setReducer(new JoinNodesAndEdgesReducer());
		driver.setKeyGroupingComparator(new NodeKeyGroupingComparator());
		driver.setKeyOrderComparator(new NodeAndEdgeKeyComparator());
	}

	@Test
	public void shouldjoinToNodeAndEdgeWithFromNode() throws Exception {
		driver.withFirstInput(new LongWritable(0), new Text("0	A	Aname")).addInput(new LongWritable(1), new Text("1	B	Bname"));
		output = driver.withSecondInput(new Text("RB"), new Text("0	A	B	0	A	Aname")).run();

		assertThat(output.size(), is(1));
		assertThat(output.get(0).getFirst(), equalTo(new Text("RB")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	B	0	A	Aname	1	B	Bname")));
	}
}
