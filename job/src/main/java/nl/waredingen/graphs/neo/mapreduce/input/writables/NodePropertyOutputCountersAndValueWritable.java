package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class NodePropertyOutputCountersAndValueWritable  implements Writable {
	private LongWritable id;
	private FullNodePropertiesWritable value;
	private long countBlockOffset;
	private long countIdOffset;
	private int partition;
	
	public static final FullNodePropertiesWritable EMPTY_VAL = new FullNodePropertiesWritable(-1, null, -1, -1, -1, -1, -1, "");
	public static final LongWritable EMPTY_ID = new LongWritable(Long.MIN_VALUE);
	
	public void setValues(LongWritable id, FullNodePropertiesWritable value) {
		this.id = id;
		this.value = value;
		this.countBlockOffset = 0;
		this.countIdOffset = 0;
		this.partition = 0;
	}
	
	public void setCounter(int partition, long blockCount, long idOffsetCount) {
		this.id = EMPTY_ID;
		this.value = EMPTY_VAL;
		this.partition = partition;
		this.countBlockOffset = blockCount;
		this.countIdOffset = idOffsetCount;
	}
	
	public long getBlockOffset() {
		return countBlockOffset;
	}

	public long getIdOffset() {
		return countIdOffset;
	}
	
	public int getPartition() {
		return partition;
	}
	
	public FullNodePropertiesWritable getValue() {
		return value;
	}
	
	public LongWritable getId() {
		return id;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		value.write(out);
		if (id.equals(EMPTY_ID)) {
			out.writeInt(partition);
			out.writeLong(countBlockOffset);
			out.writeLong(countIdOffset);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (id == null) {
			id = new LongWritable();
		}
		id.readFields(in);
		if (value == null) {
			value = new FullNodePropertiesWritable();
		}
		value.readFields(in);
		
		if (id.equals(EMPTY_ID)) {
			partition = in.readInt();
			countBlockOffset = in.readLong();
			countIdOffset = in.readLong();
		} else {
			partition = 0;
			countBlockOffset = 0;
			countIdOffset = 0;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (countBlockOffset ^ (countBlockOffset >>> 32));
		result = prime * result + (int) (countIdOffset ^ (countIdOffset >>> 32));
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + partition;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		NodePropertyOutputCountersAndValueWritable other = (NodePropertyOutputCountersAndValueWritable) obj;
		if (countBlockOffset != other.countBlockOffset)
			return false;
		if (countIdOffset != other.countIdOffset)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (partition != other.partition)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodePropertyOutputCountersAndValueWritable [id=" + id + ", value=" + value + ", countBlockOffset=" + countBlockOffset + ", countIdOffset="
				+ countIdOffset + ", partition=" + partition + "]";
	}

}
