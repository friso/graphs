package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PropertyOutputIdBlockcountValueWritable  implements Writable {
	private LongWritable id;
	private Text value;
	private long count;
	private int partition;
	
	public static final Text EMPTY_STRING = new Text("");
	public static final LongWritable EMPTY_ID = new LongWritable(Long.MIN_VALUE);
	
	public void setValues(LongWritable id, Text value) {
		this.id = id;
		this.value = value;
		if (id.equals(EMPTY_ID) && value.getLength() == 0) {
			this.count = 0;
			this.partition = 0;
		}
	}
	
	public void setCounter(int partition, long count) {
		this.id = EMPTY_ID;
		this.value = EMPTY_STRING;
		this.partition = partition;
		this.count = count;
	}
	
	public long getCount() {
		return count;
	}

	public int getPartition() {
		return partition;
	}
	
	public Text getValue() {
		return value;
	}
	
	public LongWritable getId() {
		return id;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		value.write(out);
		if (id.equals(EMPTY_ID) && value.getLength() == 0) {
			out.writeInt(partition);
			out.writeLong(count);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (id == null) {
			id = new LongWritable();
		}
		id.readFields(in);
		if (value == null) {
			value = new Text();
		}
		value.readFields(in);
		
		if (id.equals(EMPTY_ID) && value.getLength() == 0) {
			partition = in.readInt();
			count = in.readLong();
		} else {
			partition = 0;
			count = 0;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
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
		PropertyOutputIdBlockcountValueWritable other = (PropertyOutputIdBlockcountValueWritable) obj;
		if (count != other.count)
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
		return "PropertyOutputIdBlockcountValueWritable [id=" + id + ", value=" + value + ", count=" + count
				+ ", partition=" + partition + "]";
	}

	
}
