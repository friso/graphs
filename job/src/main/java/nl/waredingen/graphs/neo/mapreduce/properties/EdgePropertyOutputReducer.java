package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgePropertyOutputCountersAndValueWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgePropertiesWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

public class EdgePropertyOutputReducer extends Reducer<ByteMarkerIdPropIdWritable, EdgePropertyOutputCountersAndValueWritable, NullWritable, BytesWritable> {

	private MultipleOutputs<NullWritable, BytesWritable> mos;
	private BytesWritable outputValue = new BytesWritable();
	private MetaData metaData;

	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, BytesWritable>(context);
		metaData = Neo4JUtils.getMetaData(context.getConfiguration());
	}

	protected void reduce(ByteMarkerIdPropIdWritable key, Iterable<EdgePropertyOutputCountersAndValueWritable> values, Context context) throws IOException,
			InterruptedException {
		Iterator<EdgePropertyOutputCountersAndValueWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		long blockCountOffset = 1;
		long propCountOffset = metaData.getNumberOfNodeProperties();
		EdgePropertyOutputCountersAndValueWritable value = itr.next();
		while (itr.hasNext() && value.getBlockOffset() != -1L && value.getIdOffset() != -1L) {
			blockCountOffset += value.getBlockOffset();
			propCountOffset += value.getIdOffset();
			value = itr.next();
		}

		long blocksProcessed = 0L;
		long edgeId = -1L;
		if (!value.getValue().equals(EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL)) {
			if (edgeId != value.getValue().getEdgeId().get()) {
				Text edgeText = new Text();
				edgeText.set(value.getValue().getEdgeId().get() + "\t" + value.getValue().getFromNodeIdentifier().toString() + "\t" + value.getValue().getToNodeIdentifier().toString() + "\t" + propCountOffset);
				byte[] ba = edgeText.getBytes();
				outputValue.set(ba, 0, edgeText.getLength());
				mos.write("edges", NullWritable.get(), outputValue);
				edgeId = value.getValue().getEdgeId().get();
			}
			blocksProcessed = processValue(value, blockCountOffset, propCountOffset++);
			blockCountOffset += blocksProcessed;
			context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);
			context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "edge.properties").increment(1);
		}
		while (itr.hasNext()) {
			value = itr.next();
			if (!value.getValue().equals(EdgePropertyOutputCountersAndValueWritable.EMPTY_VAL)) {
				if (edgeId != value.getValue().getEdgeId().get()) {
					Text edgeText = new Text();
					edgeText.set(value.getValue().getEdgeId().get() + "\t" + value.getValue().getFromNodeIdentifier().toString() + "\t" + value.getValue().getToNodeIdentifier().toString() + "\t" + propCountOffset);
					byte[] ba = edgeText.getBytes();
					outputValue.set(ba, 0, edgeText.getLength());
					mos.write("edges", NullWritable.get(), outputValue);
					edgeId = value.getValue().getEdgeId().get();
				}
				blocksProcessed = processValue(value, blockCountOffset, propCountOffset++);
				blockCountOffset += blocksProcessed;
				context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").increment(blocksProcessed);
				context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "edge.properties").increment(1);
			}
		}
	}

	private long processValue(EdgePropertyOutputCountersAndValueWritable value, long blockCountOffset, long propOffset) throws IOException,
			InterruptedException {

		FullEdgePropertiesWritable propHolder = value.getValue();
		int propIndex = propHolder.getPropertyIndex().get();

		PropertyRecord record = new PropertyRecord(propOffset);
		record.setInUse(true);
		record.setCreated();
		record.setInUse(true);
		long prev = propHolder.getPrevProp().get();
		long next = propHolder.getNextProp().get();
		if (prev != -1L) {
			prev = (prev - propIndex) + propOffset;
		}
		if (next != -1L) {
			next = (next - propIndex) + propOffset;
		}
		record.setPrevProp(prev);
		record.setNextProp(next);

		String[] properties = propHolder.getProperties().valuesToArray();
		int[] propertyKeyIndexes = propHolder.getProperties().keysToArray();

		for (int i = 0; i < properties.length; i++) {
			Object propertyObj = getValueAsPropertyTypedClass(propertyKeyIndexes[i], properties[i]);
			PropertyBlock propertyBlock = getPropertyBlock(propertyKeyIndexes[i], propertyObj, blockCountOffset);
			record.addPropertyBlock(propertyBlock);
		}

		byte[] ba = Neo4JUtils.getPropertyReferenceAsByteArray(record);
		outputValue.set(ba, 0, ba.length);
		mos.write("props", NullWritable.get(), outputValue);

		for (PropertyBlock block : record.getPropertyBlocks()) {

			if (block.getValueRecords().size() > 0) {
				ba = Neo4JUtils.getDynamicRecordsAsByteArray(block.getValueRecords(), 128);
				outputValue.set(ba, 0, ba.length);
				mos.write("strings", NullWritable.get(), outputValue);
			}
		}

		return propHolder.getBlockCount().get();

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object getValueAsPropertyTypedClass(int propIndex, String property) {
		Class type = null;
		try {
			if (propIndex - metaData.getNodePropertySize() + 2 < metaData.getEdgePropertySize()) {
				type = metaData.getEdgeTypes()[propIndex - metaData.getNodePropertySize() + 2];
			}
			if (type != null) {
				return type.getConstructor(String.class).newInstance(property);
			} else {
				return property;
			}
		} catch (Exception e) {
			return property;
		}
	}

	private PropertyBlock getPropertyBlock(int propertyKey, Object property, long blockOffset) {
		PropertyBlock block = new PropertyBlock();
		Neo4JUtils.encodeValue(block, propertyKey, property, blockOffset);
		return block;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
