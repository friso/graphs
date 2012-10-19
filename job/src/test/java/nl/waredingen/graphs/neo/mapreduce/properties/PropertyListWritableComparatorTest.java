package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nl.waredingen.graphs.neo.mapreduce.input.writables.PropertyListWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.junit.Test;

public class PropertyListWritableComparatorTest {

	@Test
	public void testPropertyListWritablesAreEqual() {
		PropertyListWritable e1 = new PropertyListWritable();
		e1.setKeys(0,1,2);
		e1.setValues("prop3", "prop2", "prop0");
		PropertyListWritable e2 = new PropertyListWritable();
		e2.setKeys(0,1,2);
		e2.setValues("prop3", "prop2", "prop0");
		
		assertTrue(e1.equals(e2));
		assertEquals(0, e1.compareTo(e2));
		assertEquals(0, e2.compareTo(e1));
		assertEquals(0, e1.compareTo(e1));
		assertEquals(0, e2.compareTo(e2));
	}

	@Test
	public void testPropertyListWritablesAreNonEqual() {
		PropertyListWritable e1 = new PropertyListWritable();
		e1.setKeys(0,1,2);
		e1.setValues("prop3", "prop2", "prop0");
		PropertyListWritable e2 = new PropertyListWritable();
		e2.setKeys(0,1,2);
		e2.setValues("prop3", "prop2", "prop1");
		
		assertFalse(e1.equals(e2));
		assertEquals(-1, e1.compareTo(e2));
		assertEquals(1, e2.compareTo(e1));
		assertEquals(0, e1.compareTo(e1));
		assertEquals(0, e2.compareTo(e2));
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testPropertyListWritablesAreEquallyPartitioned() {
		PropertyListWritable e1 = new PropertyListWritable();
		e1.setKeys(0,1,2);
		e1.setValues("prop3", "prop2", "prop0");
		PropertyListWritable e2 = new PropertyListWritable();
		e2.setKeys(0,1,2);
		e2.setValues("prop3", "prop2", "prop0");
		
		Partitioner p = new HashPartitioner<PropertyListWritable, Text>();
		assertEquals(p.getPartition(e1, new Text(), 50), p.getPartition(e2, new Text(), 50));
	}

}
