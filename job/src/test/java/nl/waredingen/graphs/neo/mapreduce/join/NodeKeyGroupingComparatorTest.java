package nl.waredingen.graphs.neo.mapreduce.join;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeKeyGroupingComparatorTest {

	@Test
	public void shouldEqualNodeKeyAndEdgeKey() {
		Text nodeKey = new Text("NA");
		Text edgeKey = new Text("EA");
		
		NodeKeyGroupingComparator comp = new NodeKeyGroupingComparator();

		assertThat(comp.compare(nodeKey, edgeKey), is(0));
	}

	@Test
	public void shouldEqualNodeKeyAndEdgeKeyInUnorderedWay() {
		Text nodeKey = new Text("NA");
		Text edgeKey = new Text("EA");
		
		NodeKeyGroupingComparator comp = new NodeKeyGroupingComparator();

		assertThat(comp.compare(edgeKey, nodeKey), is(0));
	}

}
