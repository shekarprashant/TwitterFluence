
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountInfluenceFollowingReducer extends MapReduceBase implements Reducer<TextPair,Text,Text,Text> {

    private float dampingFactor;
    @Override
    public void configure(JobConf conf) {
            dampingFactor = conf.getFloat("DAMPING", 0.85f);
    }
    
	@Override
	public void reduce(TextPair key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter report)
			throws IOException {
		
		 if (!values.hasNext())
             return;

		 float selfInfluence = dampingFactor * Float.parseFloat(values.next().toString());
         float sendInfluence = 0.0f;
         Text Influence;
        
         if (!values.hasNext())
                 return;

         int FollowingCount = Integer.parseInt(values.next().toString());
         if (FollowingCount == 0)
                 return;

         sendInfluence = selfInfluence / FollowingCount;

         while (values.hasNext()) {
                 Text Followed = values.next();
                 Influence = new Text(Float.toString(sendInfluence).getBytes());
                 output.collect(Followed, Influence);
         }        
         //Influence = new Text(Float.toString(selfInfluence).getBytes());
         //output.collect(key.getFirst(), Influence);		
	}
}