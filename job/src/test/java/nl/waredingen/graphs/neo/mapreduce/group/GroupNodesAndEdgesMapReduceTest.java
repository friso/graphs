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
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesMapReduceTest {
	private MapReduceDriver<NullWritable, Text, NodeEdgeIdWritable, EdgeWritable, NullWritable, NodeEdgeWritable> driver;
	private List<Pair<NullWritable, NodeEdgeWritable>> output;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		driver = new MapReduceDriver<NullWritable, Text, NodeEdgeIdWritable, EdgeWritable, NullWritable, NodeEdgeWritable>(new GroupNodesAndEdgesMapper(), new GroupNodesAndEdgesReducer());
		driver.setKeyGroupingComparator(new NodeAndEdgeIdKeyGroupingComparator());
		driver.setKeyOrderComparator(new NodeAndEdgeIdKeyComparator());
	}

	@Test
	public void shouldOutputWithFromAndToNode() throws Exception {
		output = driver.withInput(NullWritable.get(), new Text("0	A	B	5	0	1	1	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new NodeEdgeWritable(0,1,0,0,1,5)));

		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new NodeEdgeWritable(1,3,0,0,1,5)));

	}
	
	@Test
	public void shouldOutputWithSomeExtraEdgeProperties() throws Exception {
		Configuration config = new Configuration();
		config.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "identifier", "name");
		config.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "identifier");
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX +"identifier", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX +"name", String.class, Object.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "prop1", "prop2");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX +"from", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX +"to", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX +"prop1", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX +"prop2", String.class, Object.class);
		driver.setConfiguration(config);

		output = driver.withInput(NullWritable.get(), new Text("0	A	B	5	0	1	1	3")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(0).getSecond(), equalTo(new NodeEdgeWritable(0,1,0,0,1,5)));

		assertThat(output.get(1).getFirst(), equalTo(NullWritable.get()));
		assertThat(output.get(1).getSecond(), equalTo(new NodeEdgeWritable(1,3,0,0,1,5)));

	}

}
