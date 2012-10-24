package nl.waredingen.graphs.neo.mapreduce.input.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class DoubleSurroundingEdgeWritable  implements WritableComparable {

	private SurroundingEdgeWritable left = new SurroundingEdgeWritable();
	private SurroundingEdgeWritable right = new SurroundingEdgeWritable();

	public DoubleSurroundingEdgeWritable() {
		
	}

	public DoubleSurroundingEdgeWritable(SurroundingEdgeWritable left, SurroundingEdgeWritable right) {
		this.left = left;
		this.right = right;
	}
	
	public SurroundingEdgeWritable getLeft() {
		return left;
	}

	public SurroundingEdgeWritable getRight() {
		return right;
	}

	public void set(SurroundingEdgeWritable left, SurroundingEdgeWritable right) {
		this.left = left;
		this.right = right;
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
		DoubleSurroundingEdgeWritable other = (DoubleSurroundingEdgeWritable) obj;
		int leftDiff = left.compareTo(other.left);
		return (leftDiff == 0) ? right.compareTo(other.right): leftDiff; 
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
		DoubleSurroundingEdgeWritable other = (DoubleSurroundingEdgeWritable) obj;
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
		return "DoubleSurroundingEdgeWritable [left=" + left + ", right=" + right + "]";
	}
	
}
