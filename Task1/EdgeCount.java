package edu.gatech.cse6242;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.io.LongWritable;

public class EdgeCount implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run( new Configuration(), new EdgeCount(), args);
		System.exit(res);
	}
	
	private Configuration configuration;
	
	public void setConf(Configuration configuration){
		this.configuration = configuration;
	}
	public Configuration getConf(){
		return configuration;
	}
	

	
	public int run( String[] args) throws Exception {
    /*
     * Validate that two arguments were passed from the command line.
     */
		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
    
		
		Job job = Job.getInstance( getConf(), "EdgeCount");

    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
		job.setJarByClass(this.getClass());
    
    
		job.setMapperClass(EdgeMapper.class);
		job.setReducerClass(EdgeReducer.class);
    
		//Sepesify key/value
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Input
		FileInputFormat.addInputPath( job, new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job
		return job.waitForCompletion(true) ? 0 : 1;
  }
	
}
