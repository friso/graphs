package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgePropertiesWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EdgePreparePropertiesMapperTest {

	private MapDriver<Text, Text, AscLongDescLongWritable, FullEdgePropertiesWritable> driver;
	private List<Pair<AscLongDescLongWritable, FullEdgePropertiesWritable>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<Text, Text, AscLongDescLongWritable, FullEdgePropertiesWritable>(new EdgePreparePropertiesMapper());
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		driver.setConfiguration(populateConfigWithMetaData());
		output = driver.withInputKey(new Text("0")).withInputValue(new Text("A	B	Prop3	Prop4	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();

		FullEdgePropertiesWritable edge1 = new FullEdgePropertiesWritable(0, "A", "B", 0, 0, -1, -1, 5, "Prop3");
		edge1.add(6, "Prop4", 0);
		FullEdgePropertiesWritable edge2 = new FullEdgePropertiesWritable(0, "A", "B", 1, 1, -1, -1, 7, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect");
		
		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(edge1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(edge2));

	}

	@Test
	public void shouldOutputAsPropertiesWithVeryLongPropValue() throws Exception {
		driver.setConfiguration(populateConfigWithMetaData());
		output = driver.withInputKey(new Text("1")).withInputValue(new Text("B	C	Prop1	Prop2	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();
		
		FullEdgePropertiesWritable edge1 = new FullEdgePropertiesWritable(1, "B", "C", 0, 0, -1, -1, 5, "Prop1");
		edge1.add(6, "Prop2", 0);
		FullEdgePropertiesWritable edge2 = new FullEdgePropertiesWritable(1, "B", "C", 1, 2, -1, -1, 7, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect");
		
		assertThat(output.size(), is(2));
		

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(edge1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(edge2));
		
	}
	
	@Test
	public void shouldOutputAsPropertiesWithVeryLongPropValueAndLargeNodeSize() throws Exception {
		driver.setConfiguration(populateConfigWithMetaDataWithLargeNodeSize());
		output = driver.withInputKey(new Text("1")).withInputValue(new Text("B	C	Prop1	Prop2	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();
		
		FullEdgePropertiesWritable edge1 = new FullEdgePropertiesWritable(1, "B", "C", 0, 0, -1, -1, 5, "Prop1");
		edge1.add(6, "Prop2", 0);
		FullEdgePropertiesWritable edge2 = new FullEdgePropertiesWritable(1, "B", "C", 1, 2, -1, -1, 7, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect");
		
		assertThat(output.size(), is(2));
		
		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(edge1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(edge2));
		
	}
	
	@Test
	public void shouldOutputAsPropertiesWithVeryLongPropValueAndLargeNodeSizeAndHighEdgeId() throws Exception {
		driver.setConfiguration(populateConfigWithMetaDataWithLargeNodeSize());
		output = driver.withInputKey(new Text("700000000")).withInputValue(new Text("B	C	Prop1	Prop2	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();
		
		FullEdgePropertiesWritable edge1 = new FullEdgePropertiesWritable(700000000, "B", "C", 0, 0, -1, -1, 5, "Prop1");
		edge1.add(6, "Prop2", 0);
		FullEdgePropertiesWritable edge2 = new FullEdgePropertiesWritable(700000000, "B", "C", 1, 2, -1, -1, 7, "AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect");
		
		assertThat(output.size(), is(2));
		
		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(700000000L),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(edge1));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(700000000L),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(edge2));
		
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
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "prop1", "prop2", "longprop");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop1", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop2", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "longprop", String.class, Object.class);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 0L);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_EDGES, 1L);
		return config;
	}

	private Configuration populateConfigWithMetaDataWithLargeNodeSize() {
		Configuration config = new Configuration();
		config.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "id", "name", "prop3", "prop4", "longprop");
		config.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "id");
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "id", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "name", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "prop3", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "prop4", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "longprop", String.class, Object.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "prop1", "prop2", "longprop");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop1", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "prop2", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "longprop", String.class, Object.class);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 250000000L);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_EDGES, 1L);
		return config;
	}

}
