package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeSurroundMapReduceTest {

	private MapReduceDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable> driver;
	private List<Pair<NullWritable, SurroundingEdgeWritable>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new MapReduceDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable>(new EdgeSurroundMapper(),new EdgeSurroundReducer());
		driver.setKeyGroupingComparator(new AscLongDescLongKeyGroupingComparator());
		driver.setKeyOrderComparator(new AscLongDescLongKeyComparator());
	}

	@Test
	public void shouldjoinFromNodeAndEdge() throws Exception {
		driver.withInput(NullWritable.get(), new NodeEdgeWritable(0,1,0,0,1,100));
		driver.addInput(NullWritable.get(), new NodeEdgeWritable(0,1,2,0,2,120));
		driver.addInput(NullWritable.get(), new NodeEdgeWritable(1,3,0,0,1,100));
		driver.addInput(NullWritable.get(), new NodeEdgeWritable(1,3,1,1,2,110));
		driver.addInput(NullWritable.get(), new NodeEdgeWritable(2,5,1,1,2,110));
		driver.addInput(NullWritable.get(), new NodeEdgeWritable(2,5,2,0,2,120));
		output = driver.run();

		assertThat(output.size(), is(6));
		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new SurroundingEdgeWritable(0,2,0,2,120,0,-1)));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new SurroundingEdgeWritable(0,0,0,1,100,-1,2)));
		assertThat(output.get(2).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(2).getSecond(), equalTo(new SurroundingEdgeWritable(1,1,1,2,110,0,-1)));
		assertThat(output.get(3).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(3).getSecond(), equalTo(new SurroundingEdgeWritable(1,0,0,1,100,-1,1)));
		assertThat(output.get(4).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(4).getSecond(), equalTo(new SurroundingEdgeWritable(2,2,0,2,120,1,-1)));
		assertThat(output.get(5).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(5).getSecond(), equalTo(new SurroundingEdgeWritable(2,1,1,2,110,-1,2)));
	}
}
