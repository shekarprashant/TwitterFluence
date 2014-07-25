import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class CentralityDriver {	
	
	public static void main(String[] args) throws IOException {

 
	    if (args.length != 7) {
	      System.err.println("Usage: WordCount <in> <out> <num_users> <damping> <num_runs> <num_reducers> <print_intermediate>");
	      System.exit(1);
	    }
	    
	    String inputPath = args[0];
	    String outputPath = args[1];
	    int numUsers = Integer.parseInt(args[2]);
	    float dampingFactor = Float.parseFloat(args[3]);
	    int numRuns = Integer.parseInt(args[4]);
	    int numReducers = Integer.parseInt(args[5]);
	    boolean printIntermediate = (Integer.parseInt(args[6]) == 1);
	    
	    int i = 0;
	    FileSystem fs;
	    Path influencePath; 
	    
	    JobConf conf = new JobConf(CentralityDriver.class);
	    conf.setJobName("InvertInput");
	    conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(InvertInputMapper.class);
        conf.setReducerClass(InvertInputReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(inputPath + "/inverted"));
        conf.setNumReduceTasks(numReducers);
        conf.setJarByClass(CentralityDriver.class);
        JobClient.runJob(conf);
	    
	    
	    conf.setJobName("FollowerCount");
	    conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(FollowingCountMapper.class);
        conf.setReducerClass(FollowingCountReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath + "/inverted"));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/count"));
        conf.setNumReduceTasks(numReducers);
        conf.setJarByClass(CentralityDriver.class);
        JobClient.runJob(conf);

        conf = new JobConf(CentralityDriver.class);
        conf.setJobName("InitializeInfluence");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(InitializeInfluenceMapper.class);
        conf.setReducerClass(InitializeInfluenceReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath + "/inverted"));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/influence" + i));
        conf.setNumReduceTasks(numReducers);
        conf.setInt("USERS", numUsers);
        conf.setJarByClass(CentralityDriver.class);
        JobClient.runJob(conf);
        
        fs = FileSystem.get(conf);
        while (i < 2 * numRuns)
        {
            conf = new JobConf(CentralityDriver.class);
            conf.setJobName("CountInfluenceFollowingJoin");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            conf.setMapperClass(CountInfluenceFollowingMapper.class);

            conf.setReducerClass(CountInfluenceFollowingReducer.class);
            conf.setMapOutputKeyClass(TextPair.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setPartitionerClass(TextPairFirstPartitioner.class);
            conf.setOutputValueGroupingComparator(GroupByComparator.class);

            // relation table
            FileInputFormat.setInputPaths(conf, new Path(inputPath + "/inverted"));
            // rank table
            influencePath = new Path(outputPath + "/influence" + i);
            FileInputFormat.addInputPath(conf, influencePath);
            // count table
            FileInputFormat.addInputPath(conf, new Path(outputPath + "/count"));            
            FileOutputFormat.setOutputPath(conf,  new Path(outputPath + "/influence" + (i + 1)));
            conf.setNumReduceTasks(numReducers);
            conf.setFloat("DAMPING", dampingFactor);
            conf.setJarByClass(CentralityDriver.class);
            JobClient.runJob(conf);
            
            if(i!=0 && !printIntermediate && fs.exists(influencePath))
            	fs.delete(influencePath, true);
            i++;
            
            conf = new JobConf(CentralityDriver.class);
            conf.setJobName("AggregateInfluence");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapOutputKeyClass(Text.class);

            conf.setMapperClass(AggregateInfluenceMapper.class);
            conf.setReducerClass(AggregateInfluenceReducer.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            
            
            FileInputFormat.setInputPaths(conf, outputPath + "/influence" + i );            
            FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/influence" + (i + 1)));
            conf.setNumReduceTasks(numReducers);
            conf.setInt("USERS", numUsers);
            conf.setFloat("DAMPING", dampingFactor);
            conf.setJarByClass(CentralityDriver.class);
            JobClient.runJob(conf);
            
            
            influencePath = new Path(outputPath + "/influence" + i);
            if(!printIntermediate && fs.exists(influencePath))
            	fs.delete(influencePath, true);
            
            i++;
        }

        conf = new JobConf(CentralityDriver.class);
	    conf.setJobName("InvertOutput");
	    conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(InvertOutputMapper.class);
        conf.setReducerClass(InvertOutputReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(outputPath + "/influence" + i));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/influencefinal"));
        conf.setNumReduceTasks(1);
        conf.setJarByClass(CentralityDriver.class);
        JobClient.runJob(conf);
        
	    System.exit(0);
	}
}

