package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesMapperTest {
	private MapDriver<NullWritable, Text, NodeEdgeIdWritable, EdgeWritable> driver;
	private List<Pair<NodeEdgeIdWritable, EdgeWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<NullWritable, Text, NodeEdgeIdWritable, EdgeWritable>(new GroupNodesAndEdgesMapper());
	}

	@Test
	public void shouldOutputWithFromAndToNode() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("0	A	B	5	0	1	1	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(0,1,0)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(1,3,0)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

	}

	@Test
	public void shouldOutputWithFromAndToNodeAndSomeEdgeProperties() throws Exception {
		Configuration config = new Configuration();
		config.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "identifier", "name");
		config.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "identifier");
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "identifier", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "name", String.class, Object.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "prop1", "prop2");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop1", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop2", String.class, Object.class);
		driver.setConfiguration(config);
		
		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("0	A	B	5	0	1	1	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(0,1,0)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(1,3,0)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

	}

	/*
	 * RA 3 C A 2 C Cname 0 A Aname RB 0 A B 0 A Aname 1 B Bname RC 2 A C 0 A
	 * Aname 2 C Cname RC 1 B C 1 B Bname 2 C Cname
	 */

	@Test
	public void shouldOutputForMultipleInputs() throws Exception {
		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("3	C	A	5	2	1	0	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(2,1,3)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(3,2,0,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(0,3,3)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(3,2,0,5)));

		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("0	A	B	5	0	1	1	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(0,1,0)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(1,3,0)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(0,0,1,5)));

		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("2	A	C	5	0	1	2	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(0,1,2)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(2,0,2,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(2,3,2)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(2,0,2,5)));

		output = driver.withInputKey(NullWritable.get()).withInputValue(new Text("1	B	C	5	1	2	2	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new NodeEdgeIdWritable(1,2,1)));
		assertThat(output.get(0).getSecond(), equalTo(new EdgeWritable(1,1,2,5)));

		assertThat(output.get(1).getFirst(), equalTo(new NodeEdgeIdWritable(2,3,1)));
		assertThat(output.get(1).getSecond(), equalTo(new EdgeWritable(1,1,2,5)));
	}
}
