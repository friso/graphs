package nl.waredingen.graphs.neo.mapreduce.edges;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeOutputReducerTest {
	private ReduceDriver<LongWritable, Text, NullWritable, BytesWritable> driver;
	private List<Pair<NullWritable, BytesWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<LongWritable, Text, NullWritable, BytesWritable>(new EdgeOutputReducer());
	}

	@Test
	public void shouldOutputAsSomeEdge() throws Exception {
		output = driver.withInputKey(new LongWritable(1)).withInputValue(new Text("2	0	-1	2	-1	2")).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new BytesWritable(Neo4JUtils.getEdgeAsByteArray(1L, 2L, 0L, 0, -1L, 2L, -1L, 2L, -1L))));

	}
	
}
