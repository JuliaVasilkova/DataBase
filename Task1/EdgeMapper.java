package edu.gatech.cse6242;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	private LongWritable edge = new LongWritable();
	private LongWritable count = new LongWritable();
	
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
			String[] split = value.toString().split("\t+");
			try{
				edge.set(Long.parseLong(split[1]));
				count.set(Long.parseLong(split[2]));
				context.write(edge, count);
			} catch (NumberFormatException e) {
				
			}

		  }
}

