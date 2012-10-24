package nl.waredingen.graphs.neo.mapreduce.join;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeAndEdgeKeyComparatorTest {

	@Test
	public void shouldSortNodeKeyBeforeEdgeKey() {
		Text nodeKey = new Text("NA");
		Text edgeKey = new Text("EA");
		
		NodeAndEdgeKeyComparator comp = new NodeAndEdgeKeyComparator();

		assertThat(comp.compare(nodeKey, edgeKey), is(-9));
	}

	@Test
	public void shouldSortUnorderedNodeKeyBeforeEdgeKey() {
		Text nodeKey = new Text("NA");
		Text edgeKey = new Text("EA");
		
		NodeAndEdgeKeyComparator comp = new NodeAndEdgeKeyComparator();

		assertThat(comp.compare(edgeKey, nodeKey), is(9));
	}

}
