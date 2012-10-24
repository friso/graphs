package nl.waredingen.graphs.neo;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class ByteBufferOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class ByteRecordWriter<K, V> implements RecordWriter<K, V> {
		private DataOutputStream out;

		public ByteRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		public void write(K key, V value) throws IOException {
			boolean nullValue = value == null || value instanceof NullWritable;
			if (!nullValue) {
				BytesWritable bw = (BytesWritable) value;
				out.write(bw.get(), 0, bw.getSize());
			}
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			out.close();
		}

	}

	@Override
	public RecordWriter<K, V> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		Path path = FileOutputFormat.getTaskOutputPath(job, name);

		// create the file in the file system
		FileSystem fs = path.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(path, progress);

		// create our record writer with the new file
		return new ByteRecordWriter<K, V>(new DataOutputStream(fileOut));
	}
}
