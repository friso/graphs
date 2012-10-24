package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.junit.Test;

public class EdgeWritableComparatorTest {

	@Test
	public void testEdgeWritablesAreEqual() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(3, 2, 0, 130);
		
		assertTrue(e1.equals(e2));
		assertEquals(0, e1.compareTo(e2));
		assertEquals(0, e2.compareTo(e1));
		assertEquals(0, e1.compareTo(e1));
		assertEquals(0, e2.compareTo(e2));
	}

	@Test
	public void testEdgeWritablesAreEqualInTheKeyGroupingComparator() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(3, 2, 0, 130);
		
		EdgeWritableKeyGroupingComparator comp = new EdgeWritableKeyGroupingComparator();
		assertEquals(0, comp.compare(e1, e2));
		assertEquals(0, comp.compare(e2, e1));
	}
	
	@Test
	public void testEdgeWritablesAreNotEqualInTheKeyGroupingComparator() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(4, 2, 1, 140);
		
		EdgeWritableKeyGroupingComparator comp = new EdgeWritableKeyGroupingComparator();
		assertEquals(-1, comp.compare(e1, e2));
		assertEquals(1, comp.compare(e2, e1));
	}
	
	@Test
	public void testEdgeWritablesAreEqualInTheKeySortingComparator() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(3, 2, 0, 130);
		
		EdgeWritableKeyComparator comp = new EdgeWritableKeyComparator();
		assertEquals(0, comp.compare(e1, e2));
		assertEquals(0, comp.compare(e2, e1));
	}
	
	@Test
	public void testEdgeWritablesAreNotEqualInTheKeySortingComparator() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(4, 2, 1, 140);
		
		EdgeWritableKeyComparator comp = new EdgeWritableKeyComparator();
		assertEquals(-1, comp.compare(e1, e2));
		assertEquals(1, comp.compare(e2, e1));
	}
	
	@Test
	public void testEdgeWritablesAreNonEqual() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(4, 0, 2, 140);
		
		assertFalse(e1.equals(e2));
		assertEquals(-1, e1.compareTo(e2));
		assertEquals(1, e2.compareTo(e1));
		assertEquals(0, e1.compareTo(e1));
		assertEquals(0, e2.compareTo(e2));
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testEdgeWritablesAreEquallyPartitioned() {
		EdgeWritable e1 = new EdgeWritable(3, 2, 0, 130);
		EdgeWritable e2 = new EdgeWritable(3, 2, 0, 130);
		
		Partitioner p = new HashPartitioner<EdgeWritable, SurroundingEdgeWritable>();
		assertEquals(p.getPartition(e1, new SurroundingEdgeWritable(), 50), p.getPartition(e2, new SurroundingEdgeWritable(), 50));
		assertEquals(p.getPartition(e1, new SurroundingEdgeWritable(), 100), p.getPartition(e2, new SurroundingEdgeWritable(), 100));
		assertEquals(p.getPartition(e1, new SurroundingEdgeWritable(), 5), p.getPartition(e2, new SurroundingEdgeWritable(), 5));
		assertEquals(p.getPartition(e1, new SurroundingEdgeWritable(), 1), p.getPartition(e2, new SurroundingEdgeWritable(), 1));
		assertEquals(p.getPartition(e1, new SurroundingEdgeWritable(0,3,2,0,130,-1,2), 50), p.getPartition(e2, new SurroundingEdgeWritable(2,3,2,0,130,-1,2), 50));
	}

}
