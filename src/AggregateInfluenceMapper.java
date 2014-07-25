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

public class AggregateInfluenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    @Override
	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		List<String> tokenList = new ArrayList<String>();
		Text outputKey = new Text();
		Text outputValue = new Text();
		String line = value.toString();

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			tokenList.add(tokenizer.nextToken());
		}

		if (tokenList.size() >= 2) {
			outputKey.set(tokenList.get(0).getBytes());
			outputValue.set(tokenList.get(1).getBytes());
			output.collect(outputKey, outputValue);
		}

	}
}

