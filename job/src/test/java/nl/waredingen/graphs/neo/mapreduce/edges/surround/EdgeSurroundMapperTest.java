package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgeSurroundMapperTest {
	private MapDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable> driver;
	private List<Pair<AscLongDescLongWritable, EdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable>(new EdgeSurroundMapper());
	}

	@Test
	public void shouldOutputAsNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new NodeEdgeWritable(0,1,3,2,0,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0), new LongWritable(3))));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(3,2,0,5)));

	}

	@Test
	public void shouldOutputAsNodeWhereNodeIdIsTheKey() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new NodeEdgeWritable(11,21,3,2,0,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(11), new LongWritable(3))));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(3,2,0,5)));

	}

	@Test
	public void shouldOutputAsNodeWhereNodeIdIsTheKeyAndSomeEdgePropertiesArePresent() throws Exception {
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
		output = driver.withInputKey(NullWritable.get()).withInputValue(new NodeEdgeWritable(11,21,3,2,0,5)).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(11), new LongWritable(3))));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(3,2,0,5)));

	}
}
