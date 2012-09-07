package nl.waredingen.graphs.neo.mapreduce.nodes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeOutputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable outputKey = new LongWritable();
	private Text outputValue = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//nodeid	node	nodename	edgeid fromnodeid	tonodeid
		String[] vals = value.toString().split("\t");
		long id = Long.parseLong(vals[0]);
		outputKey.set(id);
		//TODO also output a real version of first properties id here
		outputValue.set(vals[3]+"\t"+String.valueOf(id * 2));
		context.write(outputKey, outputValue);
	}
}
