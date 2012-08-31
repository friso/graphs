package nl.waredingen.graphs.neo.mapreduce.group;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeAndEdgeIdKeyGroupingComparatorTest {

	@Test
	public void shouldEqualNodeKeyAndEdgeKey() {
		Text firstKey = new Text("0	A	Aname;0");
		Text secondKey = new Text("0	A	Aname;1");
		
		NodeAndEdgeIdKeyGroupingComparator comp = new NodeAndEdgeIdKeyGroupingComparator();

		assertThat(comp.compare(firstKey, secondKey), is(0));
	}

}
