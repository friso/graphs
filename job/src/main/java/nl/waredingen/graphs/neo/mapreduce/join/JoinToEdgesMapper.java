package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinToEdgesMapper extends Mapper<NullWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	
	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] values = value.toString().split("\t", 6);
		outputKey.set("E"+values[2]);
		context.write(outputKey, value);
	}
}
