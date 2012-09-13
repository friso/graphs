package nl.waredingen.graphs.neo.mapreduce.group;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyPartitioner;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeAndEdgeIdKeyPartitionerTest {

	@Test
	public void testSamePartitionForNodeAndEdgeIdKey() {
		Text firstKey = new Text("0	A	Aname;0");
		Text secondKey = new Text("0	A	Aname;1");
		
		NodeAndEdgeIdKeyPartitioner partitioner = new NodeAndEdgeIdKeyPartitioner();
		
		assertThat(partitioner.getPartition(firstKey, new Text(), 2), is(partitioner.getPartition(secondKey, new Text(), 2)));
		assertThat(partitioner.getPartition(firstKey, new Text(), 50), is(partitioner.getPartition(secondKey, new Text(), 50)));

	}
	
	@Test
	public void testNonNegativePartitionForNodeAndEdgeKey() {
		Text nodeKey = new Text("3663243826;1");
		
		NodeAndEdgeKeyPartitioner partitioner = new NodeAndEdgeKeyPartitioner();
		
		assertTrue(partitioner.getPartition(nodeKey, new Text(), 50) >= 0);
	}

}
