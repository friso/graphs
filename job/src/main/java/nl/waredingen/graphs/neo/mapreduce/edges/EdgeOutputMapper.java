package nl.waredingen.graphs.neo.mapreduce.edges;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeOutputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable outputKey = new LongWritable();
	private Text outputValue = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\t", 12);
		long node = Long.parseLong(vals[0]);
		long from = Long.parseLong(vals[2]);

		if (from == node) {
			outputKey.set(Long.parseLong(vals[1]));
			outputValue
					.set(from + "\t" + vals[3] + "\t" + vals[4] + "\t" + vals[5] + "\t" + vals[10] + "\t" + vals[11]);
			context.write(outputKey, outputValue);
		}
	}
}
