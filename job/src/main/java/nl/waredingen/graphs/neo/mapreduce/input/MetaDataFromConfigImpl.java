package nl.waredingen.graphs.neo.mapreduce.input;

import org.apache.hadoop.conf.Configuration;

public class MetaDataFromConfigImpl extends AbstractMetaData {

	public static final String METADATA_NODE_ID_NAME = "neo.db.creation.metadata.node.id.name";
	public static final String METADATA_NODE_PROPERTY_NAMES = "neo.db.creation.metadata.node.property.names";
	public static final String METADATA_NODE_PROPERTY_TYPE_PREFIX = "neo.db.creation.metadata.node.property.type.";
	public static final String METADATA_EDGE_PROPERTY_TYPE_PREFIX = "neo.db.creation.metadata.edge.property.type.";
	public static final String METADATA_EDGE_PROPERTY_NAMES = "neo.db.creation.metadata.edge.property.names";

	private final String[] nodeTypeNames;
	@SuppressWarnings("rawtypes")
	private final Class[] nodeTypes;
	private final String[] edgeTypeNames;
	@SuppressWarnings("rawtypes")
	private final Class[] edgeTypes;
	private String nodeIdName;

	public MetaDataFromConfigImpl(Configuration conf) {
		super(conf);
		nodeTypeNames = conf.getStrings(METADATA_NODE_PROPERTY_NAMES);
		nodeIdName = conf.get(METADATA_NODE_ID_NAME);
		nodeTypes = getNodeTypesFromConfig(conf);
		edgeTypeNames = conf.getStrings(METADATA_EDGE_PROPERTY_NAMES);
		edgeTypes = getEdgeTypesFromConfig(conf);
	}
	
	@SuppressWarnings("rawtypes")
	private Class[] getEdgeTypesFromConfig(Configuration conf) {
		Class[] result = new Class[getEdgeTypeNames().length];
		for (int i = 0; i < getEdgeTypeNames().length; i++) {
			result[i] = conf.getClass(METADATA_EDGE_PROPERTY_TYPE_PREFIX+getEdgeTypeNames()[i], String.class);
		}
		return result;
	}

	@SuppressWarnings("rawtypes")
	private Class[] getNodeTypesFromConfig(Configuration conf) {
		Class[] result = new Class[getNodeTypeNames().length];
		for (int i = 0; i < getNodeTypeNames().length; i++) {
			result[i] = conf.getClass(METADATA_NODE_PROPERTY_TYPE_PREFIX+getNodeTypeNames()[i], String.class);
		}
		return result;
	}

	@Override
	public boolean isDynamicTyping() {
		return false;
	}

	@Override
	public String[] getNodeTypeNames() {
		return nodeTypeNames;
	}

	@Override
	public String getNodeIdIdentifier() {
		return nodeIdName;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class[] getNodeTypes() {
		return nodeTypes;
	}

	@Override
	public String[] getEdgeTypeNames() {
		return edgeTypeNames;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class[] getEdgeTypes() {
		return edgeTypes;
	}

}
