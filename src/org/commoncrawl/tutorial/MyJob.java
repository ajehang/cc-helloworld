package org.commoncrawl.tutorial;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Simple access parser
 * 
 * @author Ali Imran Jehangiri <ali.jehabngiri@gmail.com>
 */
public class MyJob {
  
    public static void main(String[] args) throws Exception {
	if (args.length != 2) {
	    System.err.println("Usage: MyJob <input path> <output path>");
	    System.exit(-1);
	}
	Job job = new Job();
	job.setJarByClass(MyJob.class);
	job.setJobName("My Job");
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(AccessLogMapper.class);
	job.setReducerClass(AccessLogReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


