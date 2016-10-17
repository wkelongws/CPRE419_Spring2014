/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3  Exp2 ***************
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


public class Triangle extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Triangle(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/class/s14419x/lab3/patents.txt";   
		// first round Map-Reduce: for each vertex, generate its neighbor list
		String temp1 = "/scr/yuz1988/lab3/exp2/temp1";
		
		// second round Map-Reduce: for each vertex, count the number of all its triangles
		String temp2 = "/scr/yuz1988/lab3/exp2/temp2";  
				
		// third round Map-Reduce: sum the counts of all triangles
		// the Map process maps all the input into one same key and one reducer
		// so use combiner to compute the local sum      
		String output = "/scr/yuz1988/lab3/exp2/output/"; 
		
		int reduce_tasks = 8;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Lab3 Exp2 Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(Triangle.class); 
		
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
		
		Job job_two = new Job(conf, "Lab3 Exp2 Round Two"); 
		job_two.setJarByClass(Triangle.class); 
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
		FileOutputFormat.setOutputPath(job_two, new Path(temp2));
		
		// Run the job
		job_two.waitForCompletion(true);
		
		
		// Create job for round 3	
		Job job_three = new Job(conf, "Lab3 Exp2 Round Three"); 
		job_three.setJarByClass(Triangle.class); 
		// we only use one reducer as all inputs are mapped in one key
		job_three.setNumReduceTasks(1); 
		
		job_three.setOutputKeyClass(IntWritable.class); 
		job_three.setOutputValueClass(IntWritable.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_three.setMapperClass(Map_Three.class); 
		
		// Set the Combiner class
        // The combiner class reduces the mapper output locally
        // All the outputs from the mapper having the same key are reduced locally
        // This helps in reducing communication time as reducers get only
        // one tuple per key per mapper
        // For this example, the Reduce logic is good enough as the combiner logic
        // Hence we use the same class. 
        // However, this is not neccessary and you can write separate Combiner class
        job_three.setCombinerClass(Reduce_Three.class);
        
		job_three.setReducerClass(Reduce_Three.class);
		
		job_three.setInputFormatClass(TextInputFormat.class); 
		job_three.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(temp2)); 
		FileOutputFormat.setOutputPath(job_three, new Path(output));
		
		// Run the job
		job_three.waitForCompletion(true);
	
		return 0;
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and Text as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		private Text vertex = new Text();
		private Text neighbor = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			
			// step 1: break down a line into two vertexes
			String[] vertexes = line.split("\\s+");
			String vertex_1 = vertexes[0];
			String vertex_2 = vertexes[1];
			
			if (!vertex_1.equals(vertex_2)) {				
				// step 2: emit each vertex as key, the other vertex as neighbor			
				vertex.set(vertex_1);
				neighbor.set(vertex_2);
				context.write(vertex, neighbor);
				
				// vice versa
				vertex.set(vertex_2);
				neighbor.set(vertex_1);
				context.write(vertex, neighbor);
			}
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is Text and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
		
		// The reduce method
		// For each vertex as key, we generate its neighbor list
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			String neighborlist = "";
			
			for (Text val : values) {
				// neighbors are separated by comma in the list
				neighborlist = neighborlist + val.toString().trim() + ",";
			}
			
			// Use context.write to emit values
			context.write(key, new Text(neighborlist));
						
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	// Map2(vertexId, neighborlist)
	// for each vertex v in the neighbor list, emit (v, (vertexId : neighborlist))
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		// key
		private Text v = new Text();
		// value
		private Text info = new Text();
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString().trim();
			
			// split each line into 2 part, the first part is the vertex, second is its neighbor list
			String[] segments = line.split("\\s+");		
			
			// for each vertex v in the neighbor list, emit (v, (vertexId : neighborlist))
			String[] neighborlist = segments[1].trim().split(",");
			for (String neighbor : neighborlist) {
				v.set(neighbor);
				info.set(segments[0].trim() + ":" + segments[1]);
				context.write(v, info);				
			}
					
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			int count = 0;
			ArrayList<String[]> al = new ArrayList<String[]>();
			
			// as stated in the Map process, val is (vertexId : neighborlist)
			// we add the neighborlist (in String[] format) as one element to the ArrayList al
			for (Text val: values) {
				String[] tokens = val.toString().split(":");
				String[] neighborlist = tokens[1].trim().split(","); 
				String vertexId = tokens[0].trim();
				for (int i=0; i<al.size(); i++) {
					String[] list = al.get(i);
					for (String str : list) {
						if (vertexId.equals(str)) {
							// find a triangle!
							count++;
							break;
						}
					}
				}
				
				// add the neighborlist to the ArrayList al
				al.add(neighborlist);
			}

			context.write(key, new IntWritable(count));					
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, IntWritable, IntWritable>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			
			// split each line into 2 part 
			// the first part is the vertex number, second is the number of triangles		
			String[] tokens = line.split("\\s+");
			int number = Integer.parseInt(tokens[1].trim()); 
			
			context.write(new IntWritable(1), new IntWritable(number));
							
		}  // End method "map"
		
	}  // End Class Map_Three
	
	// The third Reduce class
	public static class Reduce_Three extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>  {
		
		private Text t = new Text();
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException  {						
			
			int sum = 0;
			
			for (IntWritable val : values) {
				int count = val.get();
				sum = sum + count;				
			}
			
			context.write(new IntWritable(1), new IntWritable(sum));											
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
	
}