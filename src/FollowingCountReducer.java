
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FollowingCountReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private static final String countIdentifier = "#"; 
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter report)
			throws IOException {

        int sum = 0;
        Text count = new Text();
        while (values.hasNext()) {
                sum += Integer.parseInt(values.next().toString());
        }
        count.set((Integer.toString(sum) + countIdentifier).getBytes());
        output.collect(key, count);		
	}
}