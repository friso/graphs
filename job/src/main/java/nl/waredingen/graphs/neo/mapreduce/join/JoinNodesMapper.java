package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinNodesMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private Text valAsText = new Text();

	@Override
	protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		valAsText.set(value.getBytes(), 0, value.getLength());
		String[] values = valAsText.toString().split("\t", 3);
		outputKey.set("N" + values[1]);
		outputValue.set(values[0] + "\t" + values[2]);
		context.write(outputKey, outputValue);
	}
}
