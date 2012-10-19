package nl.waredingen.graphs.neo.mapreduce.input;

public interface MetaData {
	
	public abstract boolean isDynamicTyping();
	
	public abstract String[] getNodeTypeNames();

	@SuppressWarnings("rawtypes")
	public abstract Class[] getNodeTypes();

	public abstract String[] getEdgeTypeNames();
	
	@SuppressWarnings("rawtypes")
	public abstract Class[] getEdgeTypes();
	
	public abstract String getNodeIdIdentifier();
	
	public int getNodePropertySize();

	public int getNodePropertyIndexForName(String name);

	@SuppressWarnings("rawtypes")
	public Class getNodePropertyTypeForName(String name);
	
	public int getNodeIdIndex();

	public String[] getNodePropertyNames();
	
	public int getEdgePropertySize();

	public int getEdgePropertyIndexForName(String name);
	
	@SuppressWarnings("rawtypes")
	public Class getEdgePropertyTypeForName(String name);
	
	public int getEdgeFromNodeIdIndex();

	public int getEdgeToNodeIdIndex();
	
	public String[] getEdgePropertyNames();

	public long getNumberOfNodes();

	public long getNumberOfEdges();

	public abstract long getNumberOfNodeProperties();

}
