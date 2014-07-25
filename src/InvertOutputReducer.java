import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertOutputReducer extends MapReduceBase implements Reducer<FloatWritable,Text,FloatWritable,Text> {

	@Override
	public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<FloatWritable, Text> output, Reporter report)
			throws IOException {
        while (values.hasNext()) {
                output.collect(key, values.next());
        }
	}
}
