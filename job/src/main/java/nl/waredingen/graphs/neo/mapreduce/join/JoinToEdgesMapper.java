package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinToEdgesMapper extends Mapper<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	
	protected void map(Text key, Text value, Context context) throws IOException ,InterruptedException {
		String[] values = value.toString().split("\t");
		outputKey.set("E"+values[2]);
		context.write(outputKey, value);
	}
}
