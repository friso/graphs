package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.junit.Test;

public class NodeAndEdgeIdKeyComparatorTest {

	@Test
	public void shouldSortNodeKeysOnEdgeIdAscKey() {
		NodeEdgeIdWritable firstKey = new NodeEdgeIdWritable(0,1,0);
		NodeEdgeIdWritable secondKey = new NodeEdgeIdWritable(0,1,1);
		
		NodeAndEdgeIdKeyComparator comp = new NodeAndEdgeIdKeyComparator();

		assertThat(comp.compare(firstKey, secondKey), is(-1));
	}


	@Test
	public void shouldSortNodeKeysOnKeyIfDifferent() {
		NodeEdgeIdWritable firstKey = new NodeEdgeIdWritable(0,1,0);
		NodeEdgeIdWritable secondKey = new NodeEdgeIdWritable(1,1,1);
		
		NodeAndEdgeIdKeyComparator comp = new NodeAndEdgeIdKeyComparator();

		assertThat(comp.compare(firstKey, secondKey), is(-1));
	}
}
