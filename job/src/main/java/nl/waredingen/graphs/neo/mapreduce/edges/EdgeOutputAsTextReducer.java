package nl.waredingen.graphs.neo.mapreduce.edges;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeOutputAsTextReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
	
	private Text outputValue = new Text();
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Iterator<Text> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		// only use first record per key. Rest is duplicates from the selfjoin in the previous step
		Text value = itr.next();

		String[] vals = value.toString().split("\t", 6);
		long relnum = key.get();
		long from = Long.parseLong(vals[0]);
		long to = Long.parseLong(vals[1]);
		long fromprev = Long.parseLong(vals[2]);
		long fromnext = Long.parseLong(vals[3]);
		long toprev = Long.parseLong(vals[4]);
		long tonext = Long.parseLong(vals[5]);
		long prop = -1L;

		writeEdge(relnum, from , to, 0, fromprev, fromnext, toprev, tonext, prop, context);
	}

	private void writeEdge(long relnum, long from, long to, int type, long fromprev, long fromnext, long toprev,
			long tonext, long prop, Context context) throws IOException, InterruptedException {
		//byte[] ba = Neo4JUtils.getEdgeAsByteArray(relnum, from, to, type, fromprev, fromnext, toprev, tonext, prop);
		outputValue.set(relnum + "\t"+from  + "\t"+to + "\t"+type + "\t"+fromprev + "\t"+fromnext + "\t"+toprev + "\t"+tonext + "\t"+prop);
		context.write(NullWritable.get(), outputValue);
	}

}
