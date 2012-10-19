package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodePreparePropertiesMapperTest {

	private MapDriver<Text, Text, AscLongDescLongWritable, FullNodePropertiesWritable> driver;
	private List<Pair<AscLongDescLongWritable, FullNodePropertiesWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<Text, Text, AscLongDescLongWritable, FullNodePropertiesWritable>(new NodePreparePropertiesMapper());
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		driver.setConfiguration(populateConfigWithMetaData());
		output = driver.withInputKey(new Text("0")).withInputValue(new Text("A	Aname	Prop3	Prop4	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();

		FullNodePropertiesWritable node1 = new FullNodePropertiesWritable(0, "A", 0, 0, -1, -1, 0, "A");
		node1.add(1, "Aname", 0);
		FullNodePropertiesWritable node2 = new FullNodePropertiesWritable(0, "A", 1, 0, -1, -1, 2, "Prop3");
		node2.add(3, "Prop4", 0);
		FullNodePropertiesWritable node3 = new FullNodePropertiesWritable(0, "A", 2, 1, -1, -1, 4, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect");

		assertThat(output.size(), is(3));
		
		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(node1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(node2));
		assertThat(output.get(2).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(2))));
		assertThat(output.get(2).getSecond(), equalTo(node3));

	}

	@Test
	public void shouldOutputAsPropertiesWithVeryLongPropValue() throws Exception {
		output = driver.withInputKey(new Text("1")).withInputValue(new Text("Prop1	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();
		
		FullNodePropertiesWritable node1 = new FullNodePropertiesWritable(1, "Prop1", 0, 0, -1, -1, 0, "Prop1");
		node1.add(1, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect", 2);
		assertThat(output.size(), is(1));
		

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(node1));
		
	}
	
	private Configuration populateConfigWithMetaData() {
		Configuration config = new Configuration();
		config.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "id", "name", "prop3", "prop4", "longprop");
		config.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "id");
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "id", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "name", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "prop3", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "prop4", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "longprop", String.class, Object.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", Long.class, Object.class);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 1L);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_EDGES, 0L);
		return config;
	}
	
	@Test
	public void shouldOutputAsPropertiesWithRealValues() throws Exception {
		driver.setConfiguration(populateConfigWithRealMetaData());
		output = driver.withInputKey(new Text("2")).withInputValue(new Text("384160409	1-3WH-21474    	000000111509024	A	I	A.J. HOP 	2518CT    	'S-GRAVENHAGE	STORT")).run();
		
		FullNodePropertiesWritable node1 = new FullNodePropertiesWritable(2, "384160409", 0, 0, -1, -1, 0, "384160409");
		node1.add(1, "1-3WH-21474", 0);
		FullNodePropertiesWritable node2 = new FullNodePropertiesWritable(2, "384160409", 1, 0, -1, -1, 2, "000000111509024");
		node2.add(3, "A", 0);
		node2.add(4, "I", 0);
		FullNodePropertiesWritable node3 = new FullNodePropertiesWritable(2, "384160409", 2, 0, -1, -1, 5, "A.J. HOP");
		node3.add(6, "2518CT", 0);
		FullNodePropertiesWritable node4 = new FullNodePropertiesWritable(2, "384160409", 3, 0, -1, -1, 7, "'S-GRAVENHAGE");
		node4.add(8, "STORT", 0);
		assertThat(output.size(), is(4));
		

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(2),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(node1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(2),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(node2));
		assertThat(output.get(2).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(2),new LongWritable(2))));
		assertThat(output.get(2).getSecond(), equalTo(node3));
		assertThat(output.get(3).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(2),new LongWritable(3))));
		assertThat(output.get(3).getSecond(), equalTo(node4));
		
	}
	
	private Configuration populateConfigWithRealMetaData() {
		Configuration conf = new Configuration();
		conf.set(AbstractMetaData.METADATA_NUMBER_OF_NODES, "" + 1);
		conf.set(AbstractMetaData.METADATA_NUMBER_OF_EDGES, "" + 0);
		
		conf.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);

		conf.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "rekening", "integraalklantnummer", "klantnummer", "cddklasse", "individu_organisatie_code", "naam", "postcode", "woonplaats", "label");
		conf.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "rekening");
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "rekening", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "integraalklantnummer", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "klantnummer", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "cddklasse", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "individu_organisatie_code", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "naam", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "postcode", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "woonplaats", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "label", String.class, Object.class);

		conf.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "netto", "eerste", "laatste", "aantal");
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "netto", Long.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "eerste", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "laatste", String.class, Object.class);
		conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "aantal", Long.class, Object.class);
		return conf;
	}

}
