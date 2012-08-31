package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class AscLongDescLongWritable implements WritableComparable {

	private LongWritable left = new LongWritable();
	private LongWritable right = new LongWritable();
	
	public AscLongDescLongWritable() {
		
	}
	
	public AscLongDescLongWritable(LongWritable left, LongWritable right) {
		this.left = left;
		this.right = right;
	}
	
	public void setLeft(LongWritable left) {
		this.left = left;
	}
	
	public void setRight(LongWritable right) {
		this.right = right;
	}
	
	public LongWritable getLeft() {
		return left;
	}
	
	public LongWritable getRight() {
		return right;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);		
	}

	@Override
	public int compareTo(Object obj) {
		AscLongDescLongWritable other = (AscLongDescLongWritable) obj;
		int leftDiff = left.compareTo(other.left);
		// sort on left and descending on right
		return (leftDiff == 0) ? -1 * right.compareTo(other.right) : leftDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
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
		AscLongDescLongWritable other = (AscLongDescLongWritable) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return left.toString()+"\t"+right.toString();
	}
}

