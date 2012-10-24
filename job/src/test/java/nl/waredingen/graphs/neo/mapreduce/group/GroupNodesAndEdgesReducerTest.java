package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesReducerTest {
	private ReduceDriver<NodeEdgeIdWritable, EdgeWritable, NullWritable, NodeEdgeWritable> driver;
	private List<Pair<NullWritable, NodeEdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<NodeEdgeIdWritable, EdgeWritable, NullWritable, NodeEdgeWritable>(new GroupNodesAndEdgesReducer());
	}

	@Test
	public void shouldOutputAsIs() throws Exception {
		output = driver.withInputKey(new NodeEdgeIdWritable(0,1,0)).withInputValue(new EdgeWritable(0,0,1,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new NodeEdgeWritable(0, 1,0,0,1,5)));

	}
}
