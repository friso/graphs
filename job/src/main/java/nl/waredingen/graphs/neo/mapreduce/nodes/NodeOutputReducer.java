package nl.waredingen.graphs.neo.mapreduce.nodes;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class NodeOutputReducer extends Reducer<LongWritable, Text, NullWritable, BytesWritable> {
	private BytesWritable outputValue = new BytesWritable();

	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Iterator<Text> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		// only use first record per key. Rest is sequential edges which are not needed here
		Text value = itr.next();

		String[] vals = value.toString().split("\t");
		long id = key.get();
		long relnum = Long.parseLong(vals[0]);
		long propnum = Long.parseLong(vals[1]);
		if (id == 0L) {
			// write a rootnode once
			writeNode(id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue(), context);
		}

		writeNode(id + 1L, relnum, propnum, context);
	}

	private void writeNode(long id, long relnum, long prop, Context context) throws IOException, InterruptedException {
		byte[] ba = Neo4JUtils.getNodeAsByteArray(id, relnum, prop);
		outputValue.set(ba, 0, ba.length);
		context.write(NullWritable.get(), outputValue);
	}

}