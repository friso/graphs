package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.PureMRNodesAndEdgesJob;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PropertyOutputMapper extends Mapper<LongWritable, Text, ByteMarkerPropertyIdWritable, PropertyOutputIdBlockcountValueWritable> {

	private ByteMarkerPropertyIdWritable outputKey = new ByteMarkerPropertyIdWritable();
	private PropertyOutputIdBlockcountValueWritable outputValue = new PropertyOutputIdBlockcountValueWritable();
	private long[] counters;
	private int numReduceTasks;
	private long maxIds;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		numReduceTasks = context.getNumReduceTasks();
		maxIds = Long.parseLong(context.getConfiguration().get(PureMRNodesAndEdgesJob.NUMBEROFROWS_CONFIG));

		counters = new long[numReduceTasks];
		outputKey.setMarker(new ByteWritable(RowNumberJob.VALUE_MARKER));
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] vals = value.toString().split("\t", 7);

		LongWritable id = new LongWritable(Long.parseLong(vals[1]));
		outputKey.setId(id);
		outputValue.setValues(id, new Text(StringUtils.join(vals, "\t", 2, vals.length)));
		counters[PropertyOutputIdBlockcountPartitioner.partitionForValue(outputValue, numReduceTasks, maxIds)] += Long.parseLong(vals[4]);
		context.write(outputKey, outputValue);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputKey.setMarker(new ByteWritable(RowNumberJob.COUNTER_MARKER));
		outputKey.setId(new LongWritable(Long.MIN_VALUE));
		for(int c = 0; c < counters.length - 1; c++) {
			if (counters[c] > 0) {
				outputValue.setCounter(c+1, counters[c]);
				context.write(outputKey, outputValue);
			}
			counters[c+1] += counters[c];
		}
	}
}

