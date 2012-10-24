package nl.waredingen.graphs.neo.mapreduce.nodes;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeIdPropIdWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodeOutputReducerTest {
	private ReduceDriver<LongWritable, EdgeIdPropIdWritable, NullWritable, BytesWritable> driver;
	private List<Pair<NullWritable, BytesWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<LongWritable, EdgeIdPropIdWritable, NullWritable, BytesWritable>(new NodeOutputReducer());
	}

	@Test
	public void shouldOutputAsSomeNode() throws Exception {
		output = driver.withInputKey(new LongWritable(1)).withInputValue(new EdgeIdPropIdWritable(3,0)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new BytesWritable(Neo4JUtils.getNodeAsByteArray(2L, 3L, 0L))));

	}
	
	@Test
	public void shouldOutputAsRootAndFirstNode() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new EdgeIdPropIdWritable(3,0)).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new BytesWritable(Neo4JUtils.getNodeAsByteArray(0L, -1L, -1L))));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new BytesWritable(Neo4JUtils.getNodeAsByteArray(1L, 3L, 0L))));

	}
	
}
