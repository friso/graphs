package nl.waredingen.graphs.neo.mapreduce.join;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinNodesAndEdgesReducerTest {
	private ReduceDriver<Text, Text, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new ReduceDriver<Text, Text, Text, Text>(new JoinNodesAndEdgesReducer());
	}

	@Test
	public void shouldOutputAsNode() throws Exception {
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("0	A	Aname"));
		values.add(new Text("0	A	B"));
		output = driver.withInputKey(new Text("NA")).withInputValues(values).run();

		assertThat(output.size(), is(1));

		assertThat(output.get(0).getFirst(), equalTo(new Text("RB")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	B	0	A	Aname")));

	}
}
