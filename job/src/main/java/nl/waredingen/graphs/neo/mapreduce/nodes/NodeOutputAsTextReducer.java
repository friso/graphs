package nl.waredingen.graphs.neo.mapreduce.nodes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class NodeOutputAsTextReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
	private Text outputValue = new Text();

	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Iterator<Text> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		long id = key.get();

		// walk through values and pick the smallest relnum and propid
		long relnum = Long.MAX_VALUE;
		long propnum = Long.MAX_VALUE;
		while (itr.hasNext()) {
			Text value = itr.next();
			String[] vals = value.toString().split("\t",2);
			relnum = Math.min(relnum, Long.parseLong(vals[0]));
			propnum = Math.min(propnum, Long.parseLong(vals[1]));
		}

		if (relnum == Long.MAX_VALUE) relnum = -1L;
		if (propnum == Long.MAX_VALUE) propnum = -1L;
		
		if (id == 0L) {
			// write a rootnode once
			writeNode(id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue(), context);
		}

		writeNode(id + 1L, relnum, propnum, context);
	}

	private void writeNode(long id, long relnum, long prop, Context context) throws IOException, InterruptedException {
		//byte[] ba = Neo4JUtils.getNodeAsByteArray(id, relnum, prop);
		outputValue.set(id +"\t"+relnum+"\t"+prop);
		context.write(NullWritable.get(), outputValue);
	}

}