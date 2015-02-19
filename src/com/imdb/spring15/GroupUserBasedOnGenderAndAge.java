package com.imdb.spring15;
/**
 * 
 */


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author Dinesh Appavoo
 * 
 * Q2 Find the count of female and males users in each age group
 * The age distribution is given below (same as in read me file)
 *  7: "Under 18"
 *  24: "18-24"
 *  31: "25-34"
 *  41: "35-44"
 *  51: "45-55"
 *  56: "55-61" 
 *  62: "62+"
 *  Use the users.dat file.
 * A sample output is given below
 * Age Gender and Count
 * 	7 M 200
 * 	24 F 120
 * where age is 7, gender is male and count is 200.
 *
 */
public class GroupUserBasedOnGenderAndAge {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] currentUserTuples=line.split("::");
			String keyStr = currentUserTuples[1].trim()+currentUserTuples[2].trim();
			context.write(new Text(keyStr), one);		
			
		}

	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//No. of movies  rated n
		int num;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
			{
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration(); 
	    //conf.set("inParameter", toString(args));
	    Job job = new Job(conf, "wordcount");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(GroupUserBasedOnGenderAndAge.class);
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
