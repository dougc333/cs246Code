package cs246workinggroup.hw11a;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//I didnt write this. This is a test program 
public class WordCount {

	public static class ISReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    } 
	}
	
	public static class TokenMapper extends Mapper<Object, Text, Text, IntWritable>{

	    private IntWritable  one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        word.set(itr.nextToken());
	        context.write(word, one);
	      }
	    }

	}
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(cs246workinggroup.hw11a.WordCount.class);
    job.setMapperClass(cs246workinggroup.hw11a.WordCount.TokenMapper.class);
    job.setCombinerClass(cs246workinggroup.hw11a.WordCount.ISReducer.class);
    job.setReducerClass(cs246workinggroup.hw11a.WordCount.ISReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("/tmp/test1.txt"));
    FileOutputFormat.setOutputPath(job, new Path("/tmp/outputwordcount"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}