package nl.waredingen.graphs.neo.mapreduce.input;

import org.apache.hadoop.conf.Configuration;


public class HardCodedMetaDataImpl extends AbstractMetaData {

	private final String[] nodeTypeNames = { "identifier", "name" };
	@SuppressWarnings("rawtypes")
	private final Class[] nodeTypes = { Long.class, String.class };
	private final String[] edgeTypeNames = { "from", "to" };
	@SuppressWarnings("rawtypes")
	private final Class[] edgeTypes = { Long.class, Long.class };

	public HardCodedMetaDataImpl(Configuration conf) {
		super(conf);
	}
	
	@Override
	public boolean isDynamicTyping() {
		return false;
	}

	@Override
	public String[] getNodeTypeNames() {
		return nodeTypeNames;
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

	@Override
	public String getNodeIdIdentifier() {
		return "identifier";
	}

}
