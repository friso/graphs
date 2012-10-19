package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.junit.Test;

public class NodeAndEdgeIdKeyGroupingComparatorTest {

	@Test
	public void shouldEqualNodeKeyAndEdgeKey() {
		NodeEdgeIdWritable firstKey = new NodeEdgeIdWritable(0,1,0);
		NodeEdgeIdWritable secondKey = new NodeEdgeIdWritable(0,1,1);
		
		NodeAndEdgeIdKeyGroupingComparator comp = new NodeAndEdgeIdKeyGroupingComparator();

		assertThat(comp.compare(firstKey, secondKey), is(0));
	}

}
