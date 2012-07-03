package nl.waredingen.graphs.neo;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ByteBufferScheme extends Scheme {

	@Override
	public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
			throws IOException {
		Fields sinkFields = getSinkFields();
        Tuple result = sinkFields != null ? tupleEntry.selectTuple(sinkFields) : tupleEntry.getTuple();

        ByteBuffer bb = (ByteBuffer) result.getObject(0);
        byte[] ba = bb.array();
        BytesWritable bw = new BytesWritable();
        bw.set(ba, 0, ba.length);
        outputCollector.collect(NullWritable.get(), bw);
    }
	
	@Override
	public void sinkInit(Tap tap, JobConf jobconf) throws IOException {
		jobconf.setOutputFormat(ByteBufferOutputFormat.class);
	}

	@Override
	public Tuple source(Object obj, Object obj1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sourceInit(Tap tap, JobConf jobconf) throws IOException {
		// TODO Auto-generated method stub

	}

}
