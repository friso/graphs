package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinNodesAndEdgesReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		if (!iter.hasNext()) {
			return;
		}
		
		String node = iter.next().toString();

		while (iter.hasNext()) {
			Text value = iter.next();
			String toNode = value.toString().split("\t")[2];
			outputKey.set("R"+toNode);
			outputValue.set(value.toString() + "\t" + node);
			
			context.write(outputKey, outputValue);
		}

	}

}
