package nl.waredingen.graphs.neo.mapreduce.group;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GroupNodesAndEdgesMapperTest {
	private MapDriver<Text, Text, Text, Text> driver;
	private List<Pair<Text, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<Text, Text, Text, Text>(new GroupNodesAndEdgesMapper());
	}

	@Test
	public void shouldOutputWithFromAndToNode() throws Exception {
		output = driver.withInputKey(new Text("RB")).withInputValue(new Text("0	A	B	0	A	Aname	1	B	Bname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new Text("0	A	Aname;0")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	0	1")));

		assertThat(output.get(1).getFirst(), equalTo(new Text("1	B	Bname;0")));
		assertThat(output.get(1).getSecond(), equalTo(new Text("0	0	1")));

	}

	/*
	 * RA 3 C A 2 C Cname 0 A Aname RB 0 A B 0 A Aname 1 B Bname RC 2 A C 0 A
	 * Aname 2 C Cname RC 1 B C 1 B Bname 2 C Cname
	 */

	@Test
	public void shouldOutputForMultipleInputs() throws Exception {
		output = driver.withInputKey(new Text("RA")).withInputValue(new Text("3	C	A	2	C	Cname	0	A	Aname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new Text("2	C	Cname;3")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("3	2	0")));

		assertThat(output.get(1).getFirst(), equalTo(new Text("0	A	Aname;3")));
		assertThat(output.get(1).getSecond(), equalTo(new Text("3	2	0")));

		output = driver.withInputKey(new Text("RB")).withInputValue(new Text("0	A	B	0	A	Aname	1	B	Bname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new Text("0	A	Aname;0")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	0	1")));

		assertThat(output.get(1).getFirst(), equalTo(new Text("1	B	Bname;0")));
		assertThat(output.get(1).getSecond(), equalTo(new Text("0	0	1")));

		output = driver.withInputKey(new Text("RC")).withInputValue(new Text("2	A	C	0	A	Aname	2	C	Cname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new Text("0	A	Aname;2")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("2	0	2")));

		assertThat(output.get(1).getFirst(), equalTo(new Text("2	C	Cname;2")));
		assertThat(output.get(1).getSecond(), equalTo(new Text("2	0	2")));

		output = driver.withInputKey(new Text("RC")).withInputValue(new Text("1	B	C	1	B	Bname	2	C	Cname")).run();

		assertThat(output.size(), is(2));

		assertThat(output.get(0).getFirst(), equalTo(new Text("1	B	Bname;1")));
		assertThat(output.get(0).getSecond(), equalTo(new Text("1	1	2")));

		assertThat(output.get(1).getFirst(), equalTo(new Text("2	C	Cname;1")));
		assertThat(output.get(1).getSecond(), equalTo(new Text("1	1	2")));
	}
}
