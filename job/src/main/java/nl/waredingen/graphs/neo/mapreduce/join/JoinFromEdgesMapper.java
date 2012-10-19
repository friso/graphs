package nl.waredingen.graphs.neo.mapreduce.join;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinFromEdgesMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private Text valAsText = new Text();

	@Override
	protected void map(NullWritable key, BytesWritable value, Context context) throws IOException ,InterruptedException {
		valAsText.set(value.getBytes(), 0, value.getLength());
		String[] values = valAsText.toString().split("\t", 4);

		outputKey.set("E"+values[1]);
		outputValue.set(StringUtils.join(values, "\t", 0, 4));
		context.write(outputKey, outputValue);
	}
}
