package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeSurroundMapper extends Mapper<LongWritable, Text, AscLongDescLongWritable, Text> {

	private AscLongDescLongWritable outputKey = new AscLongDescLongWritable();
	private Text outputValue = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//nodeid	node	nodename	edgeid fromnodeid	tonodeid
		String[] vals = value.toString().split("\t");
		outputKey.setLeft(new LongWritable(Long.parseLong(vals[0])));
		outputKey.setRight(new LongWritable(Long.parseLong(vals[3])));

		outputValue.set(vals[4]+"\t"+vals[5]);
		context.write(outputKey, outputValue);
	}
}
