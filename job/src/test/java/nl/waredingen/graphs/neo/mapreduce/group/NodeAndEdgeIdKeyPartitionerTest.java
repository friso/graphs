package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.junit.Test;

public class NodeAndEdgeIdKeyPartitionerTest {

	@Test
	public void testSamePartitionForNodeAndEdgeIdKey() {
		NodeEdgeIdWritable firstKey = new NodeEdgeIdWritable(0,1,0);
		NodeEdgeIdWritable secondKey = new NodeEdgeIdWritable(0,1,1);
		
		NodeAndEdgeIdKeyPartitioner partitioner = new NodeAndEdgeIdKeyPartitioner();
		
		assertThat(partitioner.getPartition(firstKey, new EdgeWritable(), 2), is(partitioner.getPartition(secondKey, new EdgeWritable(), 2)));
		assertThat(partitioner.getPartition(firstKey, new EdgeWritable(), 50), is(partitioner.getPartition(secondKey, new EdgeWritable(), 50)));

	}

	@Test
	public void testNonNegativePartitionForNodeAndEdgeKey() {
		NodeEdgeIdWritable nodeKey = new NodeEdgeIdWritable(3663243826L,1,1);

		NodeAndEdgeIdKeyPartitioner partitioner = new NodeAndEdgeIdKeyPartitioner();
		
		assertTrue(partitioner.getPartition(nodeKey, new EdgeWritable(), 50) >= 0);

	}
	

}
