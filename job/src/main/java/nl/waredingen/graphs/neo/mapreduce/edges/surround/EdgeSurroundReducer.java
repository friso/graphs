package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.SurroundingContext;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeSurroundReducer extends Reducer<AscLongDescLongWritable, Text, NullWritable, Text> {

	private Text outputValue = new Text();
	
	protected void reduce(AscLongDescLongWritable key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		Iterator<Text> iter = values.iterator();
		
		SurroundingContext edge = new SurroundingContext();
		
		while (iter.hasNext()) {
			String[] vals = iter.next().toString().split("\t");
			
			long id = key.getLeft().get();
			long from = Long.parseLong(vals[0]);
			long to = Long.parseLong(vals[1]);
			long relnum = key.getRight().get();
			if (edge.id == -1L) {
				// first call, so set current fields
				edge.id = id;
				edge.from = from;
				edge.to = to;
				edge.other = relnum;
				edge.prev = -1L; // don't know yet
				edge.next = -1L; // first call, relationships ordered descending, so last rel, so no next available

			} else if (edge.prev == -1L) {
				// not the first so current relationship will become prev in
				// context and context can be emitted and refilled with
				// current
				edge.prev = relnum;

				outputValue.set(edge.toString());
				context.write(NullWritable.get(), outputValue);

				long next = edge.other;
				edge.id = id;
				edge.from = from;
				edge.to = to;
				edge.other = relnum;
				edge.prev = -1L; // don't know yet
				edge.next = next;

			}

		}

		// write out last context
		outputValue.set(edge.toString());
		context.write(NullWritable.get(), outputValue);

	}

}
