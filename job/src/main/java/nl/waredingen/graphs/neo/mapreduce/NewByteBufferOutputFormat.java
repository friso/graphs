package nl.waredingen.graphs.neo.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewByteBufferOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class ByteRecordWriter<K, V> extends RecordWriter<K, V> {
		private DataOutputStream out;

		public ByteRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		@SuppressWarnings("deprecation")
		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			boolean nullValue = value == null || value instanceof NullWritable;
			if (!nullValue) {
				BytesWritable bw = (BytesWritable) value;
				out.write(bw.get(), 0, bw.getSize());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			out.close();
		}

	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = job.getConfiguration();

		Path file = getDefaultWorkFile(job, "");
		FileSystem fs = file.getFileSystem(conf);

		FSDataOutputStream fileOut = fs.create(file, false);

		return new ByteRecordWriter<K, V>(new DataOutputStream(fileOut));
	}

}
