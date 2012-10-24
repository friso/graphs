package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class FullNodePropertiesWritable  implements WritableComparable {

	private LongWritable nodeId = new LongWritable();
	private Text nodeIdentifier = new Text();
	private IntWritable propertyIndex = new IntWritable();
	private LongWritable blockCount = new LongWritable();
	private PropertyListWritable properties = new PropertyListWritable();
	private LongWritable prevProp = new LongWritable();
	private LongWritable nextProp = new LongWritable();

	public FullNodePropertiesWritable() {
		
	}
	
	public FullNodePropertiesWritable(long nodeId, String node, int propIndex, long blockCount, long prevProp, long nextProp, int propKeyIndex, String val) {
		this.set(nodeId, node, propIndex, blockCount, prevProp, nextProp, propKeyIndex, val);
	}

	public void set(FullNodePropertiesWritable other) {
		String[] props = other.getProperties().valuesToArray();
		int[] keys = other.getProperties().keysToArray();
		for (int i=0; i < props.length; i++) {
			if (i==0) {
				this.set(other.getNodeId().get(), other.getNodeIdentifier().toString(), other.getPropertyIndex().get(), other.getBlockCount().get(), other.getPrevProp().get(), other.getNextProp().get(), keys[i], props[i]);
			} else {
				this.add(keys[i], props[i], 0);
			}
		}
	}
	
	public void set(long nodeId, String node, int propIndex, long blockCount, long prevProp, long nextProp, int propKeyIndex, String val) {
		this.nodeId = new LongWritable(nodeId);
		if (node != null) this.nodeIdentifier = new Text(node);
		this.propertyIndex = new IntWritable(propIndex);
		this.blockCount = new LongWritable(blockCount);
		this.prevProp = new LongWritable(prevProp);
		this.nextProp = new LongWritable(nextProp);
		this.properties = new PropertyListWritable();
		this.properties.add(propKeyIndex, val);
	}

	public void add(int propKeyIndex, String string, int blockCount) {
		this.properties.add(propKeyIndex, string);
		this.blockCount = new LongWritable(blockCount + this.blockCount.get());
	}
	
	public LongWritable getNodeId() {
		return nodeId;
	}

	public Text getNodeIdentifier() {
		return nodeIdentifier;
	}

	public IntWritable getPropertyIndex() {
		return propertyIndex;
	}

	public LongWritable getBlockCount() {
		return blockCount;
	}

	public PropertyListWritable getProperties() {
		return properties;
	}

	public LongWritable getPrevProp() {
		return prevProp;
	}

	public LongWritable getNextProp() {
		return nextProp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		nodeIdentifier.write(out);
		propertyIndex.write(out);
		blockCount.write(out);
		properties.write(out);
		prevProp.write(out);
		nextProp.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		nodeIdentifier.readFields(in);
		propertyIndex.readFields(in);
		blockCount.readFields(in);
		properties.readFields(in);
		prevProp.readFields(in);
		nextProp.readFields(in);
	}

	@Override
	public int compareTo(Object o) {
		FullNodePropertiesWritable obj = (FullNodePropertiesWritable) o;
		int nodeDiff = nodeId.compareTo(obj.nodeId);
		int fromDiff = nodeIdentifier.compareTo(obj.nodeIdentifier);
		int indexDiff = propertyIndex.compareTo(obj.propertyIndex);
		int cntDiff = blockCount.compareTo(obj.blockCount);
		int propDiff = properties.compareTo(obj.properties);
		int prevDiff = prevProp.compareTo(obj.prevProp);
		int nextDiff = nextProp.compareTo(obj.nextProp);
		return (nodeDiff == 0) ? (fromDiff == 0) ? (indexDiff == 0) ? (cntDiff == 0) ? (propDiff == 0) ? (prevDiff == 0) ? nextDiff : prevDiff : propDiff : cntDiff : indexDiff : fromDiff: nodeDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((blockCount == null) ? 0 : blockCount.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
		result = prime * result + ((nodeIdentifier == null) ? 0 : nodeIdentifier.hashCode());
		result = prime * result + ((nextProp == null) ? 0 : nextProp.hashCode());
		result = prime * result + ((prevProp == null) ? 0 : prevProp.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((propertyIndex == null) ? 0 : propertyIndex.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FullNodePropertiesWritable other = (FullNodePropertiesWritable) obj;
		if (blockCount == null) {
			if (other.blockCount != null)
				return false;
		} else if (!blockCount.equals(other.blockCount))
			return false;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		if (nodeIdentifier == null) {
			if (other.nodeIdentifier != null)
				return false;
		} else if (!nodeIdentifier.equals(other.nodeIdentifier))
			return false;
		if (nextProp == null) {
			if (other.nextProp != null)
				return false;
		} else if (!nextProp.equals(other.nextProp))
			return false;
		if (prevProp == null) {
			if (other.prevProp != null)
				return false;
		} else if (!prevProp.equals(other.prevProp))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (propertyIndex == null) {
			if (other.propertyIndex != null)
				return false;
		} else if (!propertyIndex.equals(other.propertyIndex))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FullNodePropertiesWritable [nodeId=" + nodeId + ", nodeIdentifier=" + nodeIdentifier 
				+ ", propertyIndex=" + propertyIndex + ", blockCount=" + blockCount + ", properties=" + properties + ", prevProp=" + prevProp + ", nextProp="
				+ nextProp + "]";
	}

	
}