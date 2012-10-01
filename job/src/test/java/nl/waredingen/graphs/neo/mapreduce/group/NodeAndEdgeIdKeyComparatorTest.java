package nl.waredingen.graphs.neo.mapreduce.group;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NodeAndEdgeIdKeyComparatorTest {

	@Test
	public void shouldSortNodeKeysOnEdgeIdAscKey() {
		Text firstKey = new Text("0	A	Aname;0");
		Text secondKey = new Text("0	A	Aname;1");
		
		NodeAndEdgeIdKeyComparator comp = new NodeAndEdgeIdKeyComparator();

		assertThat(comp.compare(firstKey, secondKey), is(-1));
	}

	@Test
	public void shouldSortNodeKeysOnEdgeIdDescKeyAlsoIfKeyContainsSplitCharacter() {
		Text firstKey = new Text("0	A	Ana;;me;0");
		Text secondKey = new Text("0	A	Ana;;me;1");
		
		NodeAndEdgeIdKeyComparator comp = new NodeAndEdgeIdKeyComparator();

		assertThat(comp.compare(firstKey, secondKey), is(-1));
	}


	@Test
	public void shouldSortNodeKeysOnKeyIfDifferent() {
		Text firstKey = new Text("0	A	Aname;0");
		Text secondKey = new Text("1	B	Bname;1");
		
		NodeAndEdgeIdKeyComparator comp = new NodeAndEdgeIdKeyComparator();

		assertThat(comp.compare(firstKey, secondKey), is(-1));
	}
}
