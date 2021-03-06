/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3  Exp1 ***************
  *****************************************
  *****************************************
  */

import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopTen extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new TopTen(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/class/s14419x/lab3/patents.txt";   
		// first two rounds of Map-Reduce: count the one and two hops citations
		String temp1 = "/scr/yuz1988/lab3/exp1/temp1";      
		String output1 = "/scr/yuz1988/lab3/exp1/output1";  
		
		// second two rounds of Map-Reduce: find the ten largest citations
		String temp2 = "/scr/yuz1988/lab3/exp1/temp3";      
		String output2 = "/scr/yuz1988/lab3/exp1/output"; 
		
		int reduce_tasks = 4;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Lab3 Exp1 Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(TopTen.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class); 
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(Text.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Lab3 Exp1 Program Round Two"); 
		job_two.setJarByClass(TopTen.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp1)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output1));
		
		// Run the job
		job_two.waitForCompletion(true);
		
		
		// Create job for round 3	
		Job job_three = new Job(conf, "Lab3 Exp1 Program Round Three"); 
		job_three.setJarByClass(TopTen.class); 
		job_three.setNumReduceTasks(reduce_tasks); 
		
		job_three.setOutputKeyClass(IntWritable.class); 
		job_three.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_three.setMapperClass(Map_Three.class); 
		job_three.setReducerClass(Reduce_Three.class);
		
		job_three.setInputFormatClass(TextInputFormat.class); 
		job_three.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(output1)); 
		FileOutputFormat.setOutputPath(job_three, new Path(temp2));
		
		// Run the job
		job_three.waitForCompletion(true);
		
		
		// Create job for round 4	
		Job job_four = new Job(conf, "Lab3 Exp1 Program Round Four"); 
		job_four.setJarByClass(TopTen.class); 
		job_four.setNumReduceTasks(1); 
		
		job_four.setOutputKeyClass(IntWritable.class); 
		job_four.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_four.setMapperClass(Map_Four.class); 
		job_four.setReducerClass(Reduce_Four.class);
		
		job_four.setInputFormatClass(TextInputFormat.class); 
		job_four.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_four, new Path(temp2)); 
		FileOutputFormat.setOutputPath(job_four, new Path(output2));
		
		// Run the job
		job_four.waitForCompletion(true);
	
		return 0;
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		// private IntWritable one = new IntWritable(1);
		private Text vertex = new Text();
		private Text onehopinfo = new Text();
		// The map method 
		// The first MP round is similar with WordCount example, bigram count
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			
			//step 1: break down a line into two vertexes
			String[] vertexes = line.split("\\s+");
			String from_vertex = vertexes[0];
			String to_vertex = vertexes[1];
			
			//step 2: emit each vertex as from and to (twice)
			// emit vertex and "from" information
			vertex.set(to_vertex);
			String frominfo = "from " + from_vertex;
			onehopinfo.set(frominfo);
			context.write(vertex, onehopinfo);
			
			// emit vertex and "to" information
			vertex.set(from_vertex);
			String toinfo = "to " + to_vertex;
			onehopinfo.set(toinfo);
			context.write(vertex, onehopinfo);
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			String fromlist = "from:";
			String tolist = "to:";
			
			for (Text val : values) {	
				String onehopinfo = val.toString().trim();
				// split the onehop info: from/to + vertex
				String[] words = onehopinfo.split(" ");
				
				// if the first word is "from", add this vertex to the from list
				if (words[0].trim().equals("from")) {	
					// use comma to separate the vertexes in the list
					fromlist = fromlist + words[1].trim() + ","; 
				}
				// else, add to the to list
				else {
					tolist = tolist +words[1].trim() + ",";
				}	
			}
			
			String list = fromlist + "  " + tolist;
			
			context.write(key, new Text(list));
			
			// Use context.write to emit values
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		// key
		private Text vertex = new Text();
		// value
		private Text onetwohopinfo = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString().trim();
			
			// split each line into 3 part, the first part is the vertex info, second is the from list, third is the to list
			String[] segments = line.split("\\s+");
			// if to list is not empty
			if (!segments[2].equals("to:")) {
				// remove the "to:" and split the to list into vertexes
				String[] tolist = segments[2].substring(3).split(",");
				for (String tonode: tolist) {
					vertex.set(tonode);
					String info = segments[0];
					// if from list is not empty(two-hop node exist)
					if (!segments[1].equals("from:")) {
						String fromlist = segments[1].substring(5);
						info = info + "," + fromlist;				
					}
					onetwohopinfo.set(info);
					// emit key-value
					context.write(vertex, onetwohopinfo);
				}
				
			}
							
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			int count = 0;
			Text vertex = new Text();
			// unique list
			HashSet<Integer> hs = new HashSet<Integer>();
			
			for (Text val: values) {
				String onetwohoplist  = val.toString().trim();
				String[] vertexes = onetwohoplist.split(",");
				for (String str: vertexes) {
					hs.add(Integer.parseInt(str));
				}
			}
			
			count = hs.size();
			context.write(key, new IntWritable(count));
					
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, IntWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString().trim();
			
			// split each line into 2 part, the first part is the vertex number, second is the number of citations
			// divide the vertexes into 10 groups by mod 10
			String[] segments = line.split("\\s+");
			int number = Integer.parseInt(segments[0]); 
			int hashvalue = number%10;
			
			context.write(new IntWritable(hashvalue), value);
							
		}  // End method "map"
		
	}  // End Class Map_Three
	
	// The third Reduce class
	public static class Reduce_Three extends Reducer<IntWritable, Text, Text, IntWritable>  {
		private Text vertex = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			ArrayList<String> al = new ArrayList<String> ();
			for (Text val: values) {
				String str = val.toString().trim();
				al.add(str);			
			}
			// sort the list
			TopTen d = new TopTen();
			Collections.sort(al, d.new CitationComparator());
			
			// pick up the first 10 (maybe less if the al.size is less than 10) largest records
			int length;
			if (al.size() > 10)
				length = 10;
			else
				length = al.size();
			for (int i=0; i<length; i++) {
				String record = al.get(i);
				String[] tokens = record.split("\\s+");
				vertex.set(tokens[0]);
				context.write(vertex, new IntWritable(Integer.parseInt(tokens[1])));			
			}						
								
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
	
	// Descendant Comparator
	public class CitationComparator implements Comparator<String> {		
		public int compare(String str1, String str2) {
			String[] tokens1 = str1.split("\\s+");
			String[] tokens2 = str2.split("\\s+");
			int citnum1 = Integer.parseInt(tokens1[1]);
			int citnum2 = Integer.parseInt(tokens2[1]);
			if (citnum1 > citnum2)
				return -1;
			else if (citnum1 < citnum2)
				return 1;
			else
				return 0;
		}
			 
	}
		
	// The four Map Class
	public static class Map_Four extends Mapper<LongWritable, Text, IntWritable, Text>  {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {

			context.write(new IntWritable(1), value);
							
		}  // End method "map"
		
	}  // End Class Map_Four
		
	// The four Reduce class
	public static class Reduce_Four extends Reducer<IntWritable, Text, Text, IntWritable>  {
		private Text vertex = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			ArrayList<String> al = new ArrayList<String> ();
			for (Text val: values) {
				String str = val.toString().trim();
				al.add(str);			
			}
			// sort the list
			TopTen d = new TopTen();
			Collections.sort(al, d.new CitationComparator());
			
			// pick up the first 10 (maybe less if the al.size is less than 10) largest records
			int length;
			if (al.size() > 10)
				length = 10;
			else
				length = al.size();
			for (int i=0; i<length; i++) {
				String record = al.get(i);
				String[] tokens = record.split("\\s+");
				vertex.set(tokens[0]);
				context.write(vertex, new IntWritable(Integer.parseInt(tokens[1])));			
			}						
								
		}  // End method "reduce"
		
	}  // End Class Reduce_Four
	
}