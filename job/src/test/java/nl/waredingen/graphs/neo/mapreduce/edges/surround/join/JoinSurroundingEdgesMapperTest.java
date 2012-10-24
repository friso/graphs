package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinSurroundingEdgesMapperTest {
	private MapDriver<NullWritable, SurroundingEdgeWritable, EdgeWritable, SurroundingEdgeWritable> driver;
	private List<Pair<EdgeWritable, SurroundingEdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, SurroundingEdgeWritable, EdgeWritable, SurroundingEdgeWritable>(new JoinSurroundingEdgesMapper());
	}

	@Test
	public void shouldOutputAsJoin() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new SurroundingEdgeWritable(0,3,2,0,130,-1,2)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new EdgeWritable(3,2,0,130)));
		assertThat(output.get(0).getSecond(), equalTo(new SurroundingEdgeWritable(0,3,2,0,130,-1,2)));
	}

	@Test
	public void shouldOutputAsJoinForOtherNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new SurroundingEdgeWritable(2,3,2,0,130,1,-1)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new EdgeWritable(3,2,0,130)));
		assertThat(output.get(0).getSecond(), equalTo(new SurroundingEdgeWritable(2,3,2,0,130,1,-1)));
	}
	
}
