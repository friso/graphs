package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinSurroundingEdgesReducerTest {
	private ReduceDriver<EdgeWritable, SurroundingEdgeWritable, NullWritable, DoubleSurroundingEdgeWritable> driver;
	private List<Pair<NullWritable, DoubleSurroundingEdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<EdgeWritable, SurroundingEdgeWritable, NullWritable, DoubleSurroundingEdgeWritable>(new JoinSurroundingEdgesReducer());
	}

	@Test
	public void shouldOutputAsJoinedSurroundingEdges() throws Exception {
		ArrayList<SurroundingEdgeWritable> values = new ArrayList<SurroundingEdgeWritable>();
		values.add(new SurroundingEdgeWritable(0,2,0,2,120,3,0));
		values.add(new SurroundingEdgeWritable(2,2,0,2,120,3,1));
		output = driver.withInputKey(new EdgeWritable(2,0,2,120)).withInputValues(values).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(0,2,0,2,120,3,0), new SurroundingEdgeWritable(2,2,0,2,120,3,1))));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(2,2,0,2,120,3,1),new SurroundingEdgeWritable(0,2,0,2,120,3,0))));

	}

}
