
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InitializeInfluenceReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private static final String influenceIdentifier = "@";
	private Text initialValue;
	private int numUsers;
	
    public void configure(JobConf conf) {		
    	numUsers = conf.getInt("USERS", 194830);
    	initialValue = new Text(Double.toString(1.0/numUsers) + influenceIdentifier);
    }
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter report)
			throws IOException {
			output.collect(key, initialValue);		
	}
}