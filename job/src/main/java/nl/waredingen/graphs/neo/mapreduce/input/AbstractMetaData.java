package nl.waredingen.graphs.neo.mapreduce.input;

import org.apache.hadoop.conf.Configuration;

public abstract class AbstractMetaData implements MetaData {

	public static final String METADATA_NUMBER_OF_NODES = "neo.db.creation.metadata.node.count";
	public static final String METADATA_NUMBER_OF_NODE_PROPERTIES = "neo.db.creation.metadata.node.property.count";
	public static final String METADATA_NUMBER_OF_EDGES = "neo.db.creation.metadata.edge.count";
	public static final String METADATA_CLASS = "neo.db.creation.metadata.class";

	private long numberOfNodes;
	private long numberOfNodeProperties;
	private long numberOfEdges;

	public AbstractMetaData(Configuration conf) {
		numberOfNodes = conf.getLong(METADATA_NUMBER_OF_NODES, 0L);
		numberOfNodeProperties = conf.getLong(METADATA_NUMBER_OF_NODE_PROPERTIES, 0L);
		numberOfEdges = conf.getLong(METADATA_NUMBER_OF_EDGES, 0L);
	}
	

	public AbstractMetaData() {
	}

	public long getNumberOfNodes() {
		return numberOfNodes;
	}

	public long getNumberOfNodeProperties() {
		return numberOfNodeProperties;
	}
	
	public long getNumberOfEdges() {
		return numberOfEdges;
	}

	@Override
	public int getNodePropertySize() {
		return getNodeTypeNames().length;
	}

	@Override
	public int getNodePropertyIndexForName(String name) {
		return getPropertyIndexForName(name, getNodeTypeNames());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getNodePropertyTypeForName(String name) {
		int nodePropertyIndexForName = getNodePropertyIndexForName(name);
		if (nodePropertyIndexForName >= 0) {
			return getNodeTypes()[nodePropertyIndexForName];
		} else {
			return String.class;
		}
	}

	@Override
	public int getNodeIdIndex() {
		return getNodePropertyIndexForName(getNodeIdIdentifier());
	}

	@Override
	public String[] getNodePropertyNames() {
		return getNodeTypeNames();
	}

	@Override
	public int getEdgePropertySize() {
		return getEdgeTypeNames().length;
	}

	@Override
	public int getEdgePropertyIndexForName(String name) {
		return getPropertyIndexForName(name, getEdgeTypeNames());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getEdgePropertyTypeForName(String name) {
		int edgePropertyIndexForName = getEdgePropertyIndexForName(name);
		if (edgePropertyIndexForName >= 0) {
			return getEdgeTypes()[edgePropertyIndexForName];
		} else {
			return String.class;
		}
	}

	@Override
	public int getEdgeFromNodeIdIndex() {
		return getEdgePropertyIndexForName("from");
	}

	@Override
	public int getEdgeToNodeIdIndex() {
		return getEdgePropertyIndexForName("to");
	}

	@Override
	public String[] getEdgePropertyNames() {
		return getEdgeTypeNames();
	}

	private int getPropertyIndexForName(String name, String[] array) {
		int result = -1;
		for (int i = 0; i < array.length; i++) {
			if (array[i] != null && array[i].equals(name)) {
				result = i;
			}
		}
		return result;
	}


}
