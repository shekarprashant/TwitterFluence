import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class InvertOutputMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {
  

	@Override
	public void map(LongWritable key, Text value, OutputCollector<FloatWritable,Text> output, Reporter reporter) throws IOException {
		
		List<String> tokenList = new ArrayList<String>();
		FloatWritable outputKey = new FloatWritable();
		Text outputValue = new Text();
		String line = value.toString();		 
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens())
			tokenList.add(tokenizer.nextToken());

		if (tokenList.size() >= 2) {
			String value1 = tokenList.get(0);
			String value2 = tokenList.get(1); 			
			outputKey.set(Float.parseFloat(value2.substring(0,value2.length() - 1)));			
			outputValue.set(value1.getBytes());
			output.collect(outputKey,outputValue);
		}
    }
}
