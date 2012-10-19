package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeSurroundReducerTest {
	private ReduceDriver<AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable> driver;
	private List<Pair<NullWritable, SurroundingEdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable>(new EdgeSurroundReducer());
	}

	@Test
	public void shouldOutputSurroundingEdges() throws Exception {
		List<EdgeWritable> values = new ArrayList<EdgeWritable>();
		values.add(new EdgeWritable(3,2,1,130));
		values.add(new EdgeWritable(1,1,2,110));
		values.add(new EdgeWritable(2,1,3,120));
		values.add(new EdgeWritable(4,3,1,140));
		//Unfortunately no multiple keys can be added to this reducer, as is in real life.
		//This results in an incorrect relnum in this test
		output = driver.withInputKey(new AscLongDescLongWritable(new LongWritable(1), new LongWritable(3))).withInputValues(values).run();

		assertThat(output.size(), is(4));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new SurroundingEdgeWritable(1,3,2,1,130,1,-1)));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new SurroundingEdgeWritable(1,1,1,2,110,2,3)));
		assertThat(output.get(2).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(2).getSecond(), equalTo(new SurroundingEdgeWritable(1,2,1,3,120,4,1)));
		assertThat(output.get(3).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(3).getSecond(), equalTo(new SurroundingEdgeWritable(1,4,3,1,140,-1,2)));

	}

	@Test
	public void shouldOutputSurroundingEdgesWithProperties() throws Exception {
		List<EdgeWritable> values = new ArrayList<EdgeWritable>();
		values.add(new EdgeWritable(3,2,1,130));
		values.add(new EdgeWritable(1,1,2,110));
		values.add(new EdgeWritable(2,1,3,120));
		values.add(new EdgeWritable(4,3,1,140));
		
		Configuration config = new Configuration();
		config.setClass("neo.input.metadata.class", MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings("neo.input.metadata.node.property.names", "identifier", "name");
		config.set("neo.input.metadata.node.id.name", "identifier");
		config.setClass("neo.input.metadata.node.property.type.identifier", Long.class, Object.class);
		config.setClass("neo.input.metadata.node.property.type.name", String.class, Object.class);
		config.setStrings("neo.input.metadata.edge.property.names", "from", "to", "prop1", "prop2");
		config.setClass("neo.input.metadata.edge.property.type.from", Long.class, Object.class);
		config.setClass("neo.input.metadata.edge.property.type.to", Long.class, Object.class);
		config.setClass("neo.input.metadata.edge.property.type.prop1", String.class, Object.class);
		config.setClass("neo.input.metadata.edge.property.type.prop2", String.class, Object.class);
		driver.setConfiguration(config);

		//Unfortunately no multiple keys can be added to this reducer, as is in real life.
		//This results in an incorrect relnum in this test
		output = driver.withInputKey(new AscLongDescLongWritable(new LongWritable(1), new LongWritable(3))).withInputValues(values).run();
		
		assertThat(output.size(), is(4));
		
		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new SurroundingEdgeWritable(1,3,2,1,130,1,-1)));
		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new SurroundingEdgeWritable(1,1,1,2,110,2,3)));
		assertThat(output.get(2).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(2).getSecond(), equalTo(new SurroundingEdgeWritable(1,2,1,3,120,4,1)));
		assertThat(output.get(3).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(3).getSecond(), equalTo(new SurroundingEdgeWritable(1,4,3,1,140,-1,2)));
		
	}
	
}
