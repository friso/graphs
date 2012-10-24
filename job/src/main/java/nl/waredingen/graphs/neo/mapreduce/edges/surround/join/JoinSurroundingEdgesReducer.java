package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinSurroundingEdgesReducer extends Reducer<EdgeWritable, SurroundingEdgeWritable, NullWritable, DoubleSurroundingEdgeWritable> {

	private DoubleSurroundingEdgeWritable outputValue = new DoubleSurroundingEdgeWritable();

	@Override
	protected void reduce(EdgeWritable key, Iterable<SurroundingEdgeWritable> values, Context context) throws IOException, InterruptedException {
		
		Iterator<SurroundingEdgeWritable> iter = values.iterator();
		if (!iter.hasNext()) {
			return;
		}

		//Deep copy this to don't run into the pitfall of the hadoop iterable optimalization to reuse the next object
		SurroundingEdgeWritable left = new SurroundingEdgeWritable(iter.next());

		if (iter.hasNext()) {
			SurroundingEdgeWritable right = new SurroundingEdgeWritable(iter.next());

			if (!left.equals(right)) {
				outputValue.set(left,right);

				context.write(NullWritable.get(), outputValue);
				outputValue.set(right,left);

				context.write(NullWritable.get(), outputValue);
			}
		}
	}

}
