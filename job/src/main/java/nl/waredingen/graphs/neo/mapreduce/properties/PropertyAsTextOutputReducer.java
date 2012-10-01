package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

public class PropertyAsTextOutputReducer extends Reducer<ByteMarkerPropertyIdWritable, PropertyOutputIdBlockcountValueWritable, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;
	private Text outputValue = new Text();

	protected void reduce(ByteMarkerPropertyIdWritable key, Iterable<PropertyOutputIdBlockcountValueWritable> values, Context context) throws IOException,
			InterruptedException {
		System.out.println("In reduce method with key: "+key.toString());
		Iterator<PropertyOutputIdBlockcountValueWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}
		
		long offset = 1;
		PropertyOutputIdBlockcountValueWritable value = itr.next();
		while (itr.hasNext() && value.getCount() > 0) {
			System.out.println("Reducer adding offset:"+value.getCount());
			offset += value.getCount();
			value = itr.next();
		}
		System.out.println("Reducer starting offset:"+offset);
		
		long blocksProcessed = 0L;
		if (!value.getValue().equals(PropertyOutputIdBlockcountValueWritable.EMPTY_STRING)) {
			blocksProcessed = processValue(value, offset);
			offset += blocksProcessed;
			context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);
		}
		while(itr.hasNext()) {
			value = itr.next();
			if (!value.getValue().equals(PropertyOutputIdBlockcountValueWritable.EMPTY_STRING)) {
				blocksProcessed = processValue(value, offset);
				if (blocksProcessed > 0) System.out.println("Incrementing offset by "+blocksProcessed);
				offset += blocksProcessed;

				context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);
			}
		}
	}

	private long processValue(PropertyOutputIdBlockcountValueWritable value, long offset) throws IOException, InterruptedException {
		String[] vals = value.getValue().toString().split("\t", 6);
		PropertyBlock block = new PropertyBlock();
		int propId = Integer.parseInt(vals[0]);

		Neo4JUtils.encodeValue(block, propId, vals[1], offset);
		//PropertyRecord record = new PropertyRecord(propId);
		//record.setInUse(true);
		//record.setPrevProp(Long.parseLong(vals[3]));
		//record.setNextProp(Long.parseLong(vals[4]));
		//record.addPropertyBlock(block);
		//byte[] ba = Neo4JUtils.getPropertyReferenceAsByteArray(record);
		outputValue.set(propId + "\t" + vals[1] + "\t" + vals[3] + "\t" + vals[4] + "\t" + block.getSingleValueLong());
		mos.write("props", NullWritable.get(), outputValue);

		if (block.getValueRecords().size() > 0) {
			
			StringBuilder sb = new StringBuilder().append(propId).append("\t");
			int i=0;
			for (DynamicRecord dynamicRecord : block.getValueRecords()) {

				long nextProp = dynamicRecord.getNextBlock();
				int mostlyNrOfBytesInt = dynamicRecord.getLength();

				sb.append(i).append("\t").append(mostlyNrOfBytesInt).append("\t").append(nextProp).append("\t").append(new String(dynamicRecord.getData())).append("\t");
				i++;
			}
			//ba = Neo4JUtils.getDynamicRecordsAsByteArray(block.getValueRecords(), 128);
			outputValue.set(sb.toString());
			mos.write("strings", NullWritable.get(), outputValue);
		}

		return Long.parseLong(vals[2]);

	}
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
