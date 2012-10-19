package nl.waredingen.graphs.neo.mapreduce.properties;

public class SurroundingPropertyContext {

	public long nodeId = -1L, count = -1L, prev = -1L, next = -1L;
	public int index = -1; 
	public String nodeIdentifier = null;
	public String toNodeIdentifier = null;
	public String[] properties = null;
	public int[] propertyIndexes = null;

	public SurroundingPropertyContext() {
	}

}
