package nl.waredingen.graphs.neo.mapreduce.nodes;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodeOutputMapperTest {
	private MapDriver<NullWritable, NodeEdgeWritable, LongWritable, EdgeIdPropIdWritable> driver;
	private List<Pair<LongWritable, EdgeIdPropIdWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, NodeEdgeWritable, LongWritable, EdgeIdPropIdWritable>(new NodeOutputMapper());
	}

	@Test
	public void shouldOutputAsNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new NodeEdgeWritable(0,1,3,2,0,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(0)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeIdPropIdWritable(3,1)));

	}

	@Test
	public void shouldOutputAsNodeWhereNodeIdIsTheKey() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new NodeEdgeWritable(11,21,3,2,0,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(11)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeIdPropIdWritable(3,21)));

	}
}
