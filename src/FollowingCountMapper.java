import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class FollowingCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  
    private static final Text one = new Text("1");
    
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		

		List<String> tokenList = new ArrayList<String>();
		Text outputKey = new Text();
		String line = value.toString();		 
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens())
			tokenList.add(tokenizer.nextToken());

		if (tokenList.size() >= 2) {
			String userId = tokenList.get(0);
			outputKey.set(userId.getBytes());
			output.collect(outputKey, one);
		}

    }
}
