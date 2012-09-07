package nl.waredingen.graphs.neo.mapreduce.properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class NodePreparePropertiesMapperTest {

	private MapDriver<LongWritable, Text, AscLongDescLongWritable, Text> driver;
	private List<Pair<AscLongDescLongWritable, Text>> output;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<LongWritable, Text, AscLongDescLongWritable, Text>(new NodePreparePropertiesMapper());
	}

	@Test
	public void shouldOutputAsProperties() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("0	A	Aname	Prop3	Prop4	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();

		assertThat(output.size(), is(5));

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(0))));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	A	0")));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(1))));
		assertThat(output.get(1).getSecond(), equalTo(new Text("1	Aname	0")));
		assertThat(output.get(2).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(2))));
		assertThat(output.get(2).getSecond(), equalTo(new Text("2	Prop3	0")));
		assertThat(output.get(3).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(3))));
		assertThat(output.get(3).getSecond(), equalTo(new Text("3	Prop4	0")));
		assertThat(output.get(4).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(0),new LongWritable(4))));
		assertThat(output.get(4).getSecond(), equalTo(new Text("4	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect	1")));

	}
	@Test
	public void shouldOutputAsPropertiesWithVeryLongPropValue() throws Exception {
		output = driver.withInputKey(new LongWritable(0)).withInputValue(new Text("1	Prop1	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect")).run();
		
		assertThat(output.size(), is(2));
		

		assertThat(output.get(0).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(2))));
		assertThat(output.get(0).getSecond(), equalTo(new Text("0	Prop1	0")));
		assertThat(output.get(1).getFirst(), equalTo(new AscLongDescLongWritable(new LongWritable(1),new LongWritable(3))));
		assertThat(output.get(1).getSecond(), equalTo(new Text("1	AndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrectAndAPropWithAverryLongvalueBecauseWeKindOfNeedToFindOutIfABlockCountIsPresentAndCorrect	2")));
		
	}
}
