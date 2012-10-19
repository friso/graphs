package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class PropertyListWritable implements WritableComparable {

	private ArrayList<IntWritable> propertyIndexKeys = new ArrayList<IntWritable>();
	private ArrayList<Text> properties = new ArrayList<Text>();

	public PropertyListWritable() {

	}

	public PropertyListWritable(PropertyListWritable other) {
		for (Text text : other.getProperties()) {
			properties.add(new Text(text.toString()));
		}
		for (IntWritable key : other.getPropertyIndexKeys()) {
			propertyIndexKeys.add(new IntWritable(key.get()));
		}
	}

	public void setValues(String... vals) {
		for (String string : vals) {
			Text val = new Text();
			val.set(string);
			this.properties.add(val);
		}
	}

	public void setKeys(int... vals) {
		for (int key : vals) {
			IntWritable val = new IntWritable();
			val.set(key);
			this.propertyIndexKeys.add(val);
		}
	}

	public ArrayList<Text> getProperties() {
		return this.properties;
	}

	public ArrayList<IntWritable> getPropertyIndexKeys() {
		return this.propertyIndexKeys;
	}

	public String[] valuesToArray() {
		String[] result = new String[this.properties.size()];
		for (int i = 0; i < properties.size(); i++) {
			result[i] = properties.get(i).toString();
		}
		return result;
	}

	public int[] keysToArray() {
		int[] result = new int[this.propertyIndexKeys.size()];
		for (int i = 0; i < propertyIndexKeys.size(); i++) {
			result[i] = propertyIndexKeys.get(i).get();
		}
		return result;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(properties.size());
		for (IntWritable value : propertyIndexKeys) {
			value.write(out);
		}
		for (Text value : properties) {
			value.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		propertyIndexKeys = new ArrayList<IntWritable>(size);
		for (int i = 0; i < size; i++) {
			IntWritable value = new IntWritable();
			value.readFields(in);
			propertyIndexKeys.add(value);
		}
		properties = new ArrayList<Text>(size);
		for (int i = 0; i < size; i++) {
			Text value = new Text();
			value.readFields(in);
			properties.add(value);
		}

	}

	@Override
	public int compareTo(Object o) {
		PropertyListWritable obj = (PropertyListWritable) o;
		if (this.properties.equals(obj.properties) && this.propertyIndexKeys.equals(obj.propertyIndexKeys)) {
			return 0;
		} else {
			int propDiff = comparePropertiesTo(obj.properties);
			if (propDiff == 0) {
				return comparePropertyIndexesTo(obj.propertyIndexKeys);
			} else {
				return propDiff;
			}
		}
	}

	private int comparePropertiesTo(ArrayList<Text> other) {
		int result = 0;
		int sizeDiff = this.properties.size() - other.size();
		if (sizeDiff == 0) {
			for (int i = 0; i < this.properties.size(); i++) {
				result = this.properties.get(i).compareTo(other.get(i));
				if (result != 0)
					break;
			}
		} else {
			result = sizeDiff;
		}
		return result;
	}

	private int comparePropertyIndexesTo(ArrayList<IntWritable> other) {
		int result = 0;
		int sizeDiff = this.propertyIndexKeys.size() - other.size();
		if (sizeDiff == 0) {
			for (int i = 0; i < this.propertyIndexKeys.size(); i++) {
				result = this.propertyIndexKeys.get(i).compareTo(other.get(i));
				if (result != 0)
					break;
			}
		} else {
			result = sizeDiff;
		}
		return result;
	}

	public void add(int index, String prop) {
		Text val = new Text();
		val.set(prop);
		this.properties.add(val);
		IntWritable key = new IntWritable(index);
		this.propertyIndexKeys.add(key);
	}

	public int getLength() {
		return this.properties.size();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((propertyIndexKeys == null) ? 0 : propertyIndexKeys.hashCode());
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
		PropertyListWritable other = (PropertyListWritable) obj;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (propertyIndexKeys == null) {
			if (other.propertyIndexKeys != null)
				return false;
		} else if (!propertyIndexKeys.equals(other.propertyIndexKeys))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PropertyListWritable [propertyIndexKeys=" + propertyIndexKeys + ", properties=" + properties + "]";
	}

}