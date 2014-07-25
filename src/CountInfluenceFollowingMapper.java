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


public class CountInfluenceFollowingMapper extends MapReduceBase implements Mapper<LongWritable, Text, TextPair, Text> {
  
    private static final String countIdentifier = "#";
    private static final String influenceIdentifier = "@";
    
    private static final String influenceLocation = "0";
    private static final String countLocation = "1";
    private static final String followingLocation = "2";
    
	@Override
	public void map(LongWritable key, Text value, OutputCollector<TextPair, Text> output, Reporter reporter) throws IOException {
		

		List<String> tokenList = new ArrayList<String>();
		TextPair outputKey = new TextPair(new Text(), new Text());
		Text outputValue = new Text();
		String line = value.toString();		 
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens())
			tokenList.add(tokenizer.nextToken());

		if (tokenList.size() >= 2) {
			String userId = tokenList.get(0);
			String userValue = tokenList.get(1);
			outputKey.setFirst(userId);
			
			if(line.endsWith(influenceIdentifier))
			{
				outputKey.setSecond(influenceLocation);
				String influence = userValue.substring(0, userValue.length() - 1);
				outputValue.set(influence.getBytes());
			}
			else if (line.endsWith(countIdentifier))
			{
				outputKey.setSecond(countLocation);
				String count = userValue.substring(0, userValue.length() - 1);
				outputValue.set(count.getBytes());
			}
			else
			{
				outputKey.setSecond(followingLocation);
				String following = userValue;
				outputValue.set(following.getBytes());
			}
			output.collect(outputKey, outputValue);

		}

    }
}
