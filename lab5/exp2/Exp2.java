/**
 ***************************************** 
 * Cpr E 419 - Lab 5 Exp2 ****************
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


public class Exp2 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new Exp2(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		String temp = "/scr/yuz1988/lab5/exp2/temp";

		int reduce_tasks = 10;
		// Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = new Job(super.getConf(), "Lab5 Exp2 Round One");
		job_one.setJarByClass(Exp2.class);
		job_one.setNumReduceTasks(reduce_tasks);

		// The datatype of the Output Key and Value
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(LongWritable.class);

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
		Job job_two = new Job(super.getConf(), "Lab5 Exp2 Round Two");
		job_two.setJarByClass(Exp2.class);
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
	public static class Map_One extends Mapper<LongWritable, Text, Text, LongWritable> {

		// The map method
		// Find screen_name and follower_count of the tweet
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// one line is a json object
			String line = value.toString();
			JSONObject obj = (JSONObject) JSONValue.parse(line);
			JSONObject userobj = (JSONObject) obj.get("user");
			String name = (String) userobj.get("screen_name");
			Long count = (Long) userobj.get("follower_count");
			context.write(new Text(name), new LongWritable(count));

		} // End method "map"

	} // End Class Map_One

	// The reduce class
	public static class Reduce_One extends Reducer<Text, LongWritable, Text, LongWritable> {

		// The reduce method
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			long count = 0;
			// find the maximum follower_count
			for (LongWritable val : values) {
				if (val.get() > count) {
					count = val.get();
				}
			}

			context.write(key, new LongWritable(count));

		} // End method "reduce"

	} // End Class Reduce_One

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.write(new IntWritable(1), value);

		} // End method "map"

	} // End Class Map_Two

	// The second Reduce class
	public static class Reduce_Two extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String> al = new ArrayList<String>();

			for (Text val : values) {
				// one line: screen_name follower_count
				String line = val.toString().trim();
				al.add(line);
			}

			Collections.sort(al, new MyComparator());

			// output all the elements in the list (top ten most common tags)
			for (int i = 0; i < 10; i++) {
				String str = al.get(i);
				String[] tokens = str.split("\\s+");
				String tag = tokens[0];
				int count = Integer.parseInt(tokens[1]);
				context.write(new Text(tag), new IntWritable(count));
			}

		} // End method "reduce"

	} // End Class Reduce_Two

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