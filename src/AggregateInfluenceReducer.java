import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;


public class AggregateInfluenceReducer extends MapReduceBase implements
Reducer<Text, Text, Text, Text>{
        
    private float randomness;
    private int numUsers;
    private float dampingFactor;
    private static final String influenceIdentifier = "@";

    @Override
    public void configure(JobConf conf) {
    		
    		numUsers = conf.getInt("USERS", 194830);
            dampingFactor = conf.getFloat("DAMPING", 0.85f);
            randomness = (1.0f - dampingFactor) / numUsers;
    }
    
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	
    		Text outputValue = new Text();
            float totalRank = 0.0f;
            while (values.hasNext()) {
                    totalRank += Float.valueOf(values.next().toString());
            }
            totalRank = randomness + dampingFactor * totalRank;
            outputValue.set((Float.toString(totalRank) + influenceIdentifier).getBytes());
            output.collect(key, outputValue);
    }

}
