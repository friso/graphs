package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundReducer;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeSurroundMapReduceAndJoinSurroundingEdgesMapReduceTest {

	private MapReduceDriver<NullWritable, SurroundingEdgeWritable, EdgeWritable, SurroundingEdgeWritable, NullWritable, DoubleSurroundingEdgeWritable> driver2;
	private List<Pair<NullWritable, DoubleSurroundingEdgeWritable>> output2;
	private MapReduceDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable> driver;
	private List<Pair<NullWritable, SurroundingEdgeWritable>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new MapReduceDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable>(new EdgeSurroundMapper(),new EdgeSurroundReducer());
		driver.setKeyGroupingComparator(new AscLongDescLongKeyGroupingComparator());
		driver.setKeyOrderComparator(new AscLongDescLongKeyComparator());

		driver2 = new MapReduceDriver<NullWritable, SurroundingEdgeWritable, EdgeWritable, SurroundingEdgeWritable, NullWritable, DoubleSurroundingEdgeWritable>(new JoinSurroundingEdgesMapper(),new JoinSurroundingEdgesReducer());
		driver2.setKeyGroupingComparator(new EdgeWritableKeyGroupingComparator());
		driver2.setKeyOrderComparator(new EdgeWritableKeyComparator());
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

		driver2.withInput(output.get(0)).withInput(output.get(1)).withInput(output.get(2)).withInput(output.get(3)).withInput(output.get(4)).withInput(output.get(5));
		output2 = driver2.run();

		assertThat(output2.size(), is(6));
		assertThat(output2.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(0).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(0,0,0,1,100,-1,2), new SurroundingEdgeWritable(1,0,0,1,100,-1,1))));
		assertThat(output2.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(1).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(1,0,0,1,100,-1,1), new SurroundingEdgeWritable(0,0,0,1,100,-1,2))));
		assertThat(output2.get(2).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(2).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(1,1,1,2,110,0,-1), new SurroundingEdgeWritable(2,1,1,2,110,-1,2))));
		assertThat(output2.get(3).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(3).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(2,1,1,2,110,-1,2), new SurroundingEdgeWritable(1,1,1,2,110,0,-1))));
		assertThat(output2.get(4).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(4).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(0,2,0,2,120,0,-1), new SurroundingEdgeWritable(2,2,0,2,120,1,-1))));
		assertThat(output2.get(5).getFirst(), equalTo(NullWritable.get()));
		assertThat(output2.get(5).getSecond(), equalTo(new DoubleSurroundingEdgeWritable(new SurroundingEdgeWritable(2,2,0,2,120,1,-1), new SurroundingEdgeWritable(0,2,0,2,120,0,-1))));
	}
}



