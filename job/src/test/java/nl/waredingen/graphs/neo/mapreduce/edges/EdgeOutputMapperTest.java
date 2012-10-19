package nl.waredingen.graphs.neo.mapreduce.edges;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeOutputMapperTest {
	private MapDriver<NullWritable, DoubleSurroundingEdgeWritable, LongWritable, FullEdgeWritable> driver;
	private List<Pair<LongWritable, FullEdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, DoubleSurroundingEdgeWritable, LongWritable, FullEdgeWritable>(new EdgeOutputMapper());
	}

	@Test
	public void shouldOutputAsEdgeWhenNodeIdMatchesFromNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(0,0,0,1,100,2,-1),new SurroundingEdgeWritable(1,0,0,1,100,1,-1))).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(0)));
		assertThat(output.get(0).getSecond(), equalTo(new FullEdgeWritable(0,1,100,2,-1,1,-1)));

	}

	@Test
	public void shouldOutputAsEdgeWhereEdgeIdIsTheKeyAndNodeIdMatchesFromNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(2,3,2,0,130,-1,2),new SurroundingEdgeWritable(0,3,2,0,130,-1,2))).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new LongWritable(3)));
		assertThat(output.get(0).getSecond(), equalTo(new FullEdgeWritable(2,0,130,-1,2,-1,2)));

	}

	@Test
	public void shouldNotOutputAsEdgeWhenNodeIdDoesNotMatchFromNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(0,3,2,0,130,-1,2),new SurroundingEdgeWritable(2,3,2,0,130,-1,2))).run();

		assertThat(output.size(), is(0));
	}


}
