package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.SurroundingContext;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NodePreparePropertiesReducer extends Reducer<AscLongDescLongWritable, Text, NullWritable, Text> {

		private Text outputValue = new Text();
		
		protected void reduce(AscLongDescLongWritable key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
			Iterator<Text> iter = values.iterator();
			
			SurroundingContext ctx = new SurroundingContext();
			
			while (iter.hasNext()) {
				String value = iter.next().toString();
				
				long nodeId = key.getLeft().get();
				long propId = key.getRight().get();
				if (ctx.id == -1L) {
					// first call, so set current fields
					ctx.id = nodeId;
					ctx.other = propId;
					ctx.val = value;
					ctx.prev = -1L; // don't know yet
					ctx.next = -1L; // first call, relationships ordered descending, so last rel, so no next available

				} else if (ctx.prev == -1L) {
					// not the first so current relationship will become prev in
					// context and context can be emitted and refilled with
					// current
					ctx.prev = propId;

					outputValue.set(ctx.toString());
					context.write(NullWritable.get(), outputValue);

					long next = ctx.other;
					ctx.id = nodeId;
					ctx.other = propId;
					ctx.val = value;
					ctx.prev = -1L; // don't know yet
					ctx.next = next;

				}

			}

			// write out last context
			outputValue.set(ctx.toString());
			context.write(NullWritable.get(), outputValue);

		}

	}
