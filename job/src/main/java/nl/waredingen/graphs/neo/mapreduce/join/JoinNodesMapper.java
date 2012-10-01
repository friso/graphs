package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinNodesMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] values = value.toString().split("\t", 3);
		outputKey.set("N"+values[1]);
		context.write(outputKey, value);
	}
}
