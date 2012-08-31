package nl.waredingen.graphs.neo.mapreduce.join;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeAndEdgeKeyPartitionerTest {

	@Test
	public void testSamePartitionForNodeAndEdgeKey() {
		Text nodeKey = new Text("NA");
		Text edgeKey = new Text("EA");
		
		NodeAndEdgeKeyPartitioner partitioner = new NodeAndEdgeKeyPartitioner();
		
		assertThat(partitioner.getPartition(nodeKey, new Text(), 2), is(partitioner.getPartition(edgeKey, new Text(), 2)));
		assertThat(partitioner.getPartition(nodeKey, new Text(), 50), is(partitioner.getPartition(edgeKey, new Text(), 50)));

	}

}
