/**
 * 
 */
package usermovierating;

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
 * @author Dany
 *
 */
public class UsersRatedNMovies {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line=value.toString();
			String[] s=line.split("::");
			context.write(new Text(s[0]), one);
		}

	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//No. of movies  rated n
		int num;
		@Override
		protected void setup(Context context)
		{
			Configuration config=context.getConfiguration();
			num=Integer.parseInt(config.get("inParameter").toString().trim());
			
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
			{
				sum+=value.get();
			}
			if(sum>num)
				context.write(key, new IntWritable(sum));
		}
	}	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration(); 
	    conf.set("inParameter", toString(args));
	    Job job = new Job(conf, "wordcount");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(UsersRatedNMovies.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	         
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	         
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	         
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
