package com.imdb.spring15;
/**
 * 
 */


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Dinesh Appavoo
 * 
 * Q1 list all male user id whose age is less or equal to 7 .
 * Using the users.dat file, list all the male userid filtering by age. This demonstrates the use of mapreduce to filter data.
 *
 */
public class UserIdForAge {

	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		int targetAge=7;
		private static final IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] currentUserTuples=line.split("::");
			int currentAgeValue = Integer.parseInt(currentUserTuples[2].trim());
			if(currentUserTuples[1].trim().equalsIgnoreCase("M")&&currentAgeValue<=targetAge)
			{
				context.write(new Text("UserId :: "+currentUserTuples[0].trim()), one);
			}			
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			for(IntWritable value : values)
				context.write(key, new IntWritable());
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration(); 
	    //conf.set("inParameter", args[0]);
	    Job job = new Job(conf, "MovieGenreIdentifier");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(UserIdForAge.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	         
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	         
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	         
	    job.waitForCompletion(true);
	}
	
	private static String toString(String[] list)
	{
		String ret="";
		for(int i=2;i<list.length;i++)
		{
			ret=ret+list[i]+" ";
		}
		return ret;
	}

}
