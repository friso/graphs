package nl.waredingen.graphs.neo.mapreduce.input;

import static org.junit.Assert.*;

import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class MetaDataTest {

	private Configuration config;
	
	@Before
	public void setup() {
		config = new Configuration();
	}
	
	@Test
	public void testHardCodedMetaData() {
			config.setClass(AbstractMetaData.METADATA_CLASS, HardCodedMetaDataImpl.class, MetaData.class);
			MetaData md = Neo4JUtils.getMetaData(config);
			
			assertEquals(0, md.getNumberOfNodes());
			assertEquals(0, md.getNumberOfEdges());
			assertEquals(2, md.getNodePropertySize());
			assertEquals(0, md.getNodeIdIndex());
			assertArrayEquals(new String[] {"identifier", "name"}, md.getNodePropertyNames());
			assertEquals(1, md.getNodePropertyIndexForName("name"));
			assertEquals(-1, md.getNodePropertyIndexForName("unknown"));
			assertEquals(2, md.getEdgePropertySize());
			assertEquals(0, md.getEdgeFromNodeIdIndex());
			assertEquals(1, md.getEdgeToNodeIdIndex());
			assertArrayEquals(new String[] { "from", "to" }, md.getEdgePropertyNames());
			assertEquals(-1, md.getEdgePropertyIndexForName("unknown"));
			assertEquals(Long.class, md.getNodePropertyTypeForName("identifier"));
			assertEquals(String.class, md.getNodePropertyTypeForName("name"));
			assertEquals(Long.class, md.getEdgePropertyTypeForName("from"));
			assertEquals(Long.class, md.getEdgePropertyTypeForName("to"));

	}

	@Test
	public void testHardCodedMetaDataWithNodesCountAndEdgesCountFromConfig() {
			config.setClass(AbstractMetaData.METADATA_CLASS, HardCodedMetaDataImpl.class, MetaData.class);
			config.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 42L);
			config.setLong(AbstractMetaData.METADATA_NUMBER_OF_EDGES, 4242L);

			MetaData md = Neo4JUtils.getMetaData(config);
			
			assertEquals(42, md.getNumberOfNodes());
			assertEquals(4242, md.getNumberOfEdges());
			assertEquals(2, md.getNodePropertySize());
			assertEquals(0, md.getNodeIdIndex());
			assertArrayEquals(new String[] {"identifier", "name"}, md.getNodePropertyNames());
			assertEquals(1, md.getNodePropertyIndexForName("name"));
			assertEquals(-1, md.getNodePropertyIndexForName("unknown"));
			assertEquals(2, md.getEdgePropertySize());
			assertEquals(0, md.getEdgeFromNodeIdIndex());
			assertEquals(1, md.getEdgeToNodeIdIndex());
			assertArrayEquals(new String[] { "from", "to" }, md.getEdgePropertyNames());
			assertEquals(-1, md.getEdgePropertyIndexForName("unknown"));
			assertEquals(Long.class, md.getNodePropertyTypeForName("identifier"));
			assertEquals(String.class, md.getNodePropertyTypeForName("name"));
			assertEquals(Long.class, md.getEdgePropertyTypeForName("from"));
			assertEquals(Long.class, md.getEdgePropertyTypeForName("to"));

	}

	@Test
	public void testHardCodedMetaDataIsTheDefault() {
		MetaData md = Neo4JUtils.getMetaData(config);
		
		assertEquals(0, md.getNumberOfNodes());
		assertEquals(0, md.getNumberOfEdges());
		assertEquals(2, md.getNodePropertySize());
		assertEquals(0, md.getNodeIdIndex());
		assertArrayEquals(new String[] {"identifier", "name"}, md.getNodePropertyNames());
		assertEquals(1, md.getNodePropertyIndexForName("name"));
		assertEquals(-1, md.getNodePropertyIndexForName("unknown"));
		assertEquals(2, md.getEdgePropertySize());
		assertEquals(0, md.getEdgeFromNodeIdIndex());
		assertEquals(1, md.getEdgeToNodeIdIndex());
		assertArrayEquals(new String[] { "from", "to" }, md.getEdgePropertyNames());
		assertEquals(-1, md.getEdgePropertyIndexForName("unknown"));
		assertEquals(Long.class, md.getNodePropertyTypeForName("identifier"));
		assertEquals(String.class, md.getNodePropertyTypeForName("name"));
		assertEquals(Long.class, md.getEdgePropertyTypeForName("from"));
		assertEquals(Long.class, md.getEdgePropertyTypeForName("to"));
		
	}
	
	@Test
	public void testConfigPopulatedMetaData() {
		populateConfigWithMetaData();

		MetaData md = Neo4JUtils.getMetaData(config);
		
		assertEquals(42, md.getNumberOfNodes());
		assertEquals(4242, md.getNumberOfEdges());
		assertEquals(3, md.getNodePropertySize());
		assertEquals(0, md.getNodeIdIndex());
		assertArrayEquals(new String[] {"id", "name", "accountnumber"}, md.getNodePropertyNames());
		assertEquals(1, md.getNodePropertyIndexForName("name"));
		assertEquals(-1, md.getNodePropertyIndexForName("unknown"));
		assertEquals(3, md.getEdgePropertySize());
		assertEquals(0, md.getEdgeFromNodeIdIndex());
		assertEquals(1, md.getEdgeToNodeIdIndex());
		assertEquals(2, md.getEdgePropertyIndexForName("amount"));
		assertArrayEquals(new String[] { "from", "to", "amount" }, md.getEdgePropertyNames());
		assertEquals(-1, md.getEdgePropertyIndexForName("unknown"));
		assertEquals(Long.class, md.getNodePropertyTypeForName("id"));
		assertEquals(String.class, md.getNodePropertyTypeForName("name"));
		assertEquals(Integer.class, md.getEdgePropertyTypeForName("amount"));
		
	}

	private void populateConfigWithMetaData() {
		config.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "id", "name", "accountnumber");
		config.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "id");
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "id", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "name", String.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "accountnumber", Integer.class, Object.class);
		config.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "amount");
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "id", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "name", Long.class, Object.class);
		config.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "amount", Integer.class, Object.class);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_NODES, 42L);
		config.setLong(AbstractMetaData.METADATA_NUMBER_OF_EDGES, 4242L);
	}

}
