/**
  *****************************************
  * Cpr E 419 - Lab 5  Exp1  **************
  *****************************************
  */

import java.io.*;
import java.util.*;


import org.json.simple.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Exp1 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Exp1(), args);
		System.exit(res); 
		
	}
	
	public int run ( String[] args ) throws Exception {
				  
		String temp = "/scr/yuz1988/lab5/exp1/temp";       
		
		int reduce_tasks = 10;
		// Configuration conf = new Configuration();
		
		// Create job for round 1
		Job job_one = new Job(super.getConf(), "Lab5 Exp1 Round One"); 
		job_one.setJarByClass(Exp1.class); 
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key and Value
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class); 
		job_one.setOutputValueClass(IntWritable.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides Input and Output Format
		job_one.setInputFormatClass(TextInputFormat.class);  
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		FileInputFormat.addInputPath(job_one, new Path(args[0])); 
		
		// The output HDFS path for this job
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2	
		Job job_two = new Job(super.getConf(), "Lab5 Exp1 Round Two"); 
		job_two.setJarByClass(Exp1.class); 
		job_two.setNumReduceTasks(1); 
		
		job_two.setOutputKeyClass(IntWritable.class); 
		job_two.setOutputValueClass(Text.class);
		
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(args[1]));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		return 0;
		
	} // End run
	
	
	// The Map Class
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
		private IntWritable one = new IntWritable(1);
		
		// The map method 
		// Find hashtags of the tweet (excluding "YOLO")
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// one line is a json object
			String line = value.toString();			
			JSONObject obj = (JSONObject) JSONValue.parse(line);
			
			JSONArray jsonArray = (JSONArray) obj.get("hashtags");		
			if (jsonArray.size() > 0) {
				// a HashSet for hashtags, remove duplicate hashtags in the same tweet
				HashSet<String> hs = new HashSet<String> ();
				for (int i=0; i<jsonArray.size(); i++) {
					// example of tagObj: "hashtag":"TeamEVHS"
					JSONObject tagObj = (JSONObject) jsonArray.get(i);
					String tagStr = (String) tagObj.get("hashtag");
					
					// "SamsungSolve" and "Samsungsolve" are same tag
					String lowercaseTag = tagStr.toLowerCase();
					if (!lowercaseTag.equals("yolo")) {
						hs.add(lowercaseTag);
					}
				}
				
				// emit each tag as key, 1 as value
				for (String str : hs) {				
					context.write(new Text(str), one);				
				}		
			}  // End if		
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		// The reduce method
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
											throws IOException, InterruptedException  {
			int sum = 0;
			
			// add up the counts of hashtag
			for (IntWritable val : values) {			
				int value = val.get();
				sum = sum + value;		
			}
			context.write(key, new IntWritable(sum));
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			context.write(new IntWritable(1), value);
			
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<IntWritable, Text, Text, IntWritable>  {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			ArrayList<String> al = new ArrayList<String> ();
			
			for (Text val : values) {
				// one line: hashtag  count
				String line = val.toString().trim();
				al.add(line);
			}
			
			Collections.sort(al, new MyComparator());
				
			// output all the elements in the list (top ten most common tags)
			for (int i=0; i<10; i++) {
				String str = al.get(i);
				String[] tokens = str.split("\\s+");
				String tag = tokens[0]; 
				int count = Integer.parseInt(tokens[1]);
				context.write(new Text(tag), new IntWritable(count));
			}
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	// Descendant Comparator
	public static class MyComparator implements Comparator<String> {
		public int compare(String str1, String str2) {
			String[] tokens1 = str1.split("\\s+");
			String[] tokens2 = str2.split("\\s+");
			int num1 = Integer.parseInt(tokens1[1]);
			int num2 = Integer.parseInt(tokens2[1]);
			if (num1 > num2)
				return -1;
			else if (num1 < num2)
				return 1;
			else
				return 0;
		}

	}
	
}