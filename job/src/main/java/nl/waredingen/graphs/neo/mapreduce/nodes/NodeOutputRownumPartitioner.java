package nl.waredingen.graphs.neo.mapreduce.nodes;

import nl.waredingen.graphs.neo.mapreduce.AbstractRownumPartitioner;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

public class NodeOutputRownumPartitioner<K, V> extends AbstractRownumPartitioner<K, V> {

	@Override
	public long getMaxCounter() {
		return Neo4JUtils.getMetaData(conf).getNumberOfNodes();
	}

}
