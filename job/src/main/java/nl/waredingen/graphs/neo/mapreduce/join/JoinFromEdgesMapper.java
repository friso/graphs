package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinFromEdgesMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] values = value.toString().split("\t");
		outputKey.set("E"+values[1]);
		context.write(outputKey, value);
	}
}
