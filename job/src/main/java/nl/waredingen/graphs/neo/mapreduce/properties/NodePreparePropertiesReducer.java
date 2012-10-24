package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NodePreparePropertiesReducer extends Reducer<AscLongDescLongWritable, FullNodePropertiesWritable, LongWritable, FullNodePropertiesWritable> {

	private LongWritable outputKey = new LongWritable();
	private FullNodePropertiesWritable outputValue = new FullNodePropertiesWritable();

	protected void reduce(AscLongDescLongWritable key, Iterable<FullNodePropertiesWritable> values, Context context) throws IOException ,InterruptedException {
			Iterator<FullNodePropertiesWritable> iter = values.iterator();
			
			SurroundingPropertyContext ctx = new SurroundingPropertyContext();
			
			while (iter.hasNext()) {
				FullNodePropertiesWritable value = iter.next();
				
				long nodeId = key.getLeft().get();
				if (ctx.nodeId == -1L) {
					// first call, so set current fields
					ctx.nodeId = nodeId;
					ctx.index = value.getPropertyIndex().get();
					ctx.nodeIdentifier = value.getNodeIdentifier().toString();
					ctx.propertyIndexes = value.getProperties().keysToArray();
					ctx.properties = value.getProperties().valuesToArray();
					ctx.count = value.getBlockCount().get();
					ctx.prev = -1L; // don't know yet
					ctx.next = -1L; // first call, relationships ordered descending, so last rel, so no next available

				} else if (ctx.prev == -1L) {
					// not the first so current relationship will become prev in
					// context and context can be emitted and refilled with
					// current
					ctx.prev = value.getPropertyIndex().get();

					outputKey.set(ctx.nodeId);
					for (int i = 0; i < ctx.properties.length; i++) {
						if (i == 0) {
							outputValue.set(ctx.nodeId, ctx.nodeIdentifier, ctx.index, ctx.count, ctx.prev, ctx.next, ctx.propertyIndexes[i],
									ctx.properties[i]);
						} else {
							outputValue.add(ctx.propertyIndexes[i], ctx.properties[i], 0);
						}
					}
					context.write(outputKey, outputValue);

					long next = ctx.index;
					ctx.nodeId = nodeId;
					ctx.index = value.getPropertyIndex().get();
					ctx.nodeIdentifier = value.getNodeIdentifier().toString();
					ctx.propertyIndexes = value.getProperties().keysToArray();
					ctx.properties = value.getProperties().valuesToArray();
					ctx.count = value.getBlockCount().get();
					ctx.prev = -1L; // don't know yet
					ctx.next = next;

				}

			}

			// write out last context
			outputKey.set(ctx.nodeId);
			for (int i = 0; i < ctx.properties.length; i++) {
				if (i == 0) {
					outputValue.set(ctx.nodeId, ctx.nodeIdentifier, ctx.index, ctx.count, ctx.prev, ctx.next, ctx.propertyIndexes[i],
							ctx.properties[i]);
				} else {
					outputValue.add(ctx.propertyIndexes[i], ctx.properties[i], 0);
				}
			}
			context.write(outputKey, outputValue);

		}
}
