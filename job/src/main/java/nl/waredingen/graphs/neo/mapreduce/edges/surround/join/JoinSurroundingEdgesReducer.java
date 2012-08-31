package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinSurroundingEdgesReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		if (!iter.hasNext()) {
			return;
		}

		String left = iter.next().toString();

		if (iter.hasNext()) {
			String right = iter.next().toString();

			if (!left.equals(right)) {
				outputValue.set(left + "\t" + right);

				context.write(NullWritable.get(), outputValue);
				outputValue.set(right + "\t" + left);

				context.write(NullWritable.get(), outputValue);
			}
		}

	}

}
