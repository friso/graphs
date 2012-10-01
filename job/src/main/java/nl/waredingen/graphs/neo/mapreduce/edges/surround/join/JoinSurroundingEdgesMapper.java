package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSurroundingEdgesMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] values = value.toString().split("\t", 5);
		
		outputKey.set(values[1]+";"+values[2]+";"+values[3]);
		context.write(outputKey, value);
	}
}
