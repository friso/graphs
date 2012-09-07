package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.PureMRNodesAndEdgesJob;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PropertyOutputMapper extends Mapper<LongWritable, Text, ByteWritable, PropertyOutputIdBlockcountValueWritable> {

	private ByteWritable outputKey = new ByteWritable();
	private PropertyOutputIdBlockcountValueWritable outputValue = new PropertyOutputIdBlockcountValueWritable();
	private long[] counters;
	private int numReduceTasks;
	private long maxIds;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		numReduceTasks = context.getNumReduceTasks();
		maxIds = Long.parseLong(context.getConfiguration().get(PureMRNodesAndEdgesJob.NUMBEROFROWS_CONFIG));

		counters = new long[numReduceTasks];
		outputKey.set(RowNumberJob.VALUE_MARKER);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] vals = value.toString().split("\t");

		outputValue.setValues(new LongWritable(Long.parseLong(vals[1])), new Text(StringUtils.join(vals, "\t", 2, vals.length)));
		context.write(outputKey, outputValue);
		long blockCount = Long.parseLong(vals[4]);
		counters[PropertyOutputIdBlockcountPartitioner.partitionForValue(outputValue, numReduceTasks, maxIds)] += blockCount ;
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputKey.set(RowNumberJob.COUNTER_MARKER);
		for(int c = 0; c < counters.length - 1; c++) {
			if (counters[c] >= 0) {
				outputValue.setCounter(c + 1, counters[c]);
				context.write(outputKey, outputValue);
			}
			counters[c + 1] += counters[c];
		}
	}
}

