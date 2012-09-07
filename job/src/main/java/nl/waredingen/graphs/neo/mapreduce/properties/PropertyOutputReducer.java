package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

public class PropertyOutputReducer extends Reducer<ByteWritable, PropertyOutputIdBlockcountValueWritable, NullWritable, BytesWritable> {

	private MultipleOutputs<NullWritable, BytesWritable> mos;
	private BytesWritable outputValue = new BytesWritable();

	protected void reduce(ByteWritable key, Iterable<PropertyOutputIdBlockcountValueWritable> values, Context context) throws IOException,
			InterruptedException {
		Iterator<PropertyOutputIdBlockcountValueWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}
		
		long offset = 1;
		PropertyOutputIdBlockcountValueWritable value = itr.next();
		while (itr.hasNext() && value.getCount() > 0) {
			offset += value.getCount();
			value = itr.next();
		}
		
		long blocksProcessed = processValue(value, offset);
		offset += blocksProcessed;
		context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);

		while(itr.hasNext()) {
			value = itr.next();
			blocksProcessed = processValue(value, offset);
			offset += blocksProcessed;

			context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);
		}
	}

	private long processValue(PropertyOutputIdBlockcountValueWritable value, long offset) throws IOException, InterruptedException {
		String[] vals = StringUtils.split(value.getValue().toString(), "\t");
		PropertyBlock block = new PropertyBlock();
		int propId = Integer.parseInt(vals[0]);

		Neo4JUtils.encodeValue(block, propId, vals[1], offset);
		PropertyRecord record = new PropertyRecord(propId);
		record.setInUse(true);
		record.setPrevProp(Long.parseLong(vals[3]));
		record.setNextProp(Long.parseLong(vals[4]));
		record.addPropertyBlock(block);
		byte[] ba = Neo4JUtils.getPropertyReferenceAsByteArray(record);
		outputValue.set(ba, 0, ba.length);
		mos.write("props", NullWritable.get(), outputValue);

		if (block.getValueRecords().size() > 0) {
			ba = Neo4JUtils.getDynamicRecordsAsByteArray(block.getValueRecords(), 128);
			outputValue.set(ba, 0, ba.length);
			mos.write("strings", NullWritable.get(), outputValue);
		}

		return Long.parseLong(vals[2]);

	}
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, BytesWritable>(context);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
