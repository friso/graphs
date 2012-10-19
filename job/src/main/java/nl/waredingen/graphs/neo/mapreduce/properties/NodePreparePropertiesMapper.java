package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

public class NodePreparePropertiesMapper extends Mapper<Text, Text, AscLongDescLongWritable, FullNodePropertiesWritable> {
	private AscLongDescLongWritable outputKey = new AscLongDescLongWritable();
	private FullNodePropertiesWritable outputValue = new FullNodePropertiesWritable();
	private MetaData metaData;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		metaData = Neo4JUtils.getMetaData(context.getConfiguration());
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t", metaData.getNodePropertySize());
		long nodeId = Long.parseLong(key.toString());
		int propId = 0;
		List<PropertyRecord> propRecords = new ArrayList<PropertyRecord>();
		Map<Integer,String> props = new HashMap<Integer, String>();
		PropertyRecord record = new PropertyRecord(propId);
		record.setInUse(true);
		record.setCreated();
		propRecords.add(record);
		for (int i = 0; i < values.length; i++) {
			String property = values[i].trim();
			int propertyKey = i;
			props.put(propertyKey, property);
			Object propertyObj = getValueAsPropertyTypedClass(propertyKey, property);
			PropertyBlock propertyBlock = getPropertyBlock(propertyKey, propertyObj);
			if (record.size() + propertyBlock.getSize() > PropertyType.getPayloadSize()) {
				PropertyRecord prevRecord = record; 
				record = new PropertyRecord(++propId);
				record.setInUse(true);
				record.setCreated();
				prevRecord.setNextProp(propId);
				record.setPrevProp(prevRecord.getId());
				propRecords.add(record);
			}
			record.addPropertyBlock(propertyBlock);
		}

		for (PropertyRecord rec : propRecords) {
			outputKey.setLeft(new LongWritable(nodeId));
			outputKey.setRight(new LongWritable(rec.getId()));
			boolean first = true;
			for (PropertyBlock block : rec.getPropertyBlocks()) {
				if (first) {
					outputValue.set(nodeId, values[metaData.getNodeIdIndex()], (int)rec.getId(), getBlockCount(block), -1L, -1L, block.getKeyIndexId(), props.get(block.getKeyIndexId()));
				first = false;
				} else {
					outputValue.add(block.getKeyIndexId(), props.get(block.getKeyIndexId()), getBlockCount(block));
				}
			}
			context.write(outputKey, outputValue);
		}
	}

	private int getBlockCount(PropertyBlock block) {
		return block.getValueRecords().size();
	}

	private PropertyBlock getPropertyBlock(int propertyKey, Object property) {
		PropertyBlock block = new PropertyBlock();
		Neo4JUtils.encodeValue(block, propertyKey, property, 0L);
		return block;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object getValueAsPropertyTypedClass(int propIndex, String property) {
		Class type = null;
		try {
			if (propIndex < metaData.getNodePropertySize()) {
				type = metaData.getNodeTypes()[propIndex];
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
}
