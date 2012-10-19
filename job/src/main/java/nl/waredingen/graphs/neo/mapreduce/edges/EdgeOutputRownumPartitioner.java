package nl.waredingen.graphs.neo.mapreduce.edges;

import nl.waredingen.graphs.neo.mapreduce.AbstractRownumPartitioner;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

public class EdgeOutputRownumPartitioner<K, V> extends AbstractRownumPartitioner<K, V> {

	@Override
	public long getMaxCounter() {
		return Neo4JUtils.getMetaData(conf).getNumberOfEdges();
	}

}
