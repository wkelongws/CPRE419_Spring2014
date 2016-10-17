/**
 ***************************************** 
 * Cpr E 419 - Lab 5 Exp3 ****************
 ***************************************** 
 */

import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class Exp3 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new Exp3(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		String input = "/class/s14419x/lab5/usa.json";
		String temp = "/scr/yuz1988/lab5/exp3/temp";
		String output = "/scr/yuz1988/lab5/exp3/output";
		
		int reduce_tasks = 10;

		// Create job for round 1
		Job job_one = new Job(super.getConf(), "Lab5 Exp3 Round One");
		job_one.setJarByClass(Exp3.class);
		job_one.setNumReduceTasks(reduce_tasks);

		// The datatype of the Output Key and Value
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides Input and Output Format
		job_one.setInputFormatClass(MyInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		FileInputFormat.addInputPath(job_one, new Path(input));

		// The output HDFS path for this job
		FileOutputFormat.setOutputPath(job_one, new Path(temp));

		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		Job job_two = new Job(super.getConf(), "Lab5 Exp3 Round Two");
		job_two.setJarByClass(Exp3.class);
		job_two.setNumReduceTasks(1);

		job_two.setOutputKeyClass(IntWritable.class);
		job_two.setOutputValueClass(Text.class);

		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);

		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);

		return 0;

	} // End run

	// The Map Class
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

		// The map method
		// Find user's name and the hashtags in that tweet
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			JSONObject obj = (JSONObject) JSONValue.parse(line);
			
			// find the name of this tweet			
			JSONObject userobj = (JSONObject) obj.get("user");
			String name = (String) userobj.get("screen_name");

			// find the hashtags in this tweet
			String hashtags = "";
			JSONObject entities = (JSONObject) obj.get("entities");
			JSONArray hashtagsArray = (JSONArray) entities.get("hashtags");
			if (hashtagsArray.size() > 0) {
				for (int i = 0; i < hashtagsArray.size(); i++) {
					JSONObject tagObj = (JSONObject) hashtagsArray.get(i);
					String tagStr = (String) tagObj.get("text");
					// if tag is not "usa", append tagStr to the hashtags, seperate by comma ","
					if (!tagStr.equalsIgnoreCase("usa")) {	
						// convert all tags to lower cases
						// hashtags = hashtags + tagStr.trim().toLowerCase() + ",";						
						hashtags = hashtags + tagStr.trim() + ",";
					}
				}		
			}	
			context.write(new Text(name), new Text(hashtags));
			
		} // End method "map"

	} // End Class Map_One

	// The reduce class
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {

		// The reduce method
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// number of posts
			int count = 0;
			// most commonly used hashtag, and the frequency
			String mostTag = "";
			int freq = 0;
			
			// put all the hashtags in a HashMap
			HashMap<String, Integer> hm = new HashMap<String, Integer> ();
			for (Text val : values) {
				count++;
				String[] hashtags = val.toString().split(",");
				for (String tag : hashtags) {
					// tag already in the HashMap, add the count
					if (hm.containsKey(tag)) {
						hm.put(tag, hm.get(tag) + 1);
						if (hm.get(tag) > freq) {
							freq = hm.get(tag);
							mostTag = tag;
						}
					}
					// new tag
					else {
						hm.put(tag,1);
						if (freq == 0) {
							mostTag = tag;
							freq = 1;							
						}
					}
					
				}
			}
			
			// key is the user name
			// the value emit is: (post count, most commonly used hashtag, frequency) 
			String valueEmit = count + "," + mostTag + "," + freq; 
			Text t = new Text(valueEmit);
			context.write(key, t);		

		} // End method "reduce"

	} // End Class Reduce_One

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// one line: screen_name   post count,most common tag,freq
			// append the screen_name to the last
			String line = value.toString();
			String[] strArray = line.split("\\s+");
			String newValue = strArray[1] + "," + strArray[0];
			
			context.write(new IntWritable(1), new Text(newValue));

		} // End method "map"

	} // End Class Map_Two

	// The second Reduce class
	public static class Reduce_Two extends Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String> al = new ArrayList<String>();

			for (Text val : values) {
				// one line: post count,most common tag,freq,screen_name
				String line = val.toString().trim();
				al.add(line);
			}

			// sort by the post count
			Collections.sort(al, new MyComparator());

			// output all the elements in the list (top ten most common tags)
			for (int i = 0; i < 10; i++) {
				String str = al.get(i);
				
				// Reformat the output
				String[] tokens = str.split(",");
				String name = tokens[3] + " (number of post: " + tokens[0] + ")";
				String tag = tokens[1] + " (freq: " + tokens[2] + ")";
				context.write(new Text(name), new Text(tag));
			}

		} // End method "reduce"

	} // End Class Reduce_Two

	// Descendant Comparator
	public static class MyComparator implements Comparator<String> {
		public int compare(String str1, String str2) {
			String[] tokens1 = str1.split(",");
			String[] tokens2 = str2.split(",");
			int num1 = Integer.parseInt(tokens1[0]);
			int num2 = Integer.parseInt(tokens2[0]);
			if (num1 > num2)
				return -1;
			else if (num1 < num2)
				return 1;
			else
				return 0;
		}

	}
	
	
	// my InputFormat
	public static class MyInputFormat extends FileInputFormat<LongWritable, Text> {

		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
				throws IOException, InterruptedException {

			return new MyRecordReader();
		}

	}
	
	// my RecordReader
	public static class MyRecordReader extends RecordReader<LongWritable, Text> {
		
		private long start;
		private long end;
		private long pos;
		private LineReader in;
		private int maxLineLength;
		
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		
		private boolean firstflag = true;
		
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}	
		}

		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
		
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
		}

		
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			// This InputSplit is a FileInputSplit
			FileSplit split = (FileSplit) inputSplit;
			// Retrieve configuration, and Max allowed bytes for a single record
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			
			// Get the start and end position of this split
			start = split.getStart();
			end = start + split.getLength();
			
			// Retrieve file containing split
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			// Retrieve the InputStream of the split
			FSDataInputStream fileIn = fs.open(split.getPath());
			
			in = new LineReader(fileIn, job);
			Text line = new Text();
			
			// Find the starting point of a JSON object
			// Use " {" as the criteria, flag is true when start is found 
			// pos is the actual start
			pos = start;
			int offset;
			boolean flag = false;
			while (flag == false) {
				line.clear();
				offset = in.readLine(line);
				String str = line.toString();
				if (str.equals(" {")) {
					flag = true;
				}
				else {
					pos += offset;
				}
			}
		}

		
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
			// Current offset is the key
	        key.set(pos);
			
	        // newSize is the size of each line that we read
	        int newSize = 0;
	        Text line = new Text();
	        String jsonObj = "";	        		
	        // 1 represents "{", -1 represents "}"
			int brace_count = 0;
	        
			if (firstflag == true) {
				jsonObj = " {";
				brace_count = 1;
				firstflag = false;
			}
			
			
			while (pos < end) {
				line.clear();
				// Read a new line and store its content to "line"
	            newSize = in.readLine(line, maxLineLength,
	                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
	            String str = line.toString();	         
	            //String trimStr = str.replaceAll("\\s+",""); 	
	            String trimStr = str.trim();
	            
	            // overlook the "][" between two jSON objects
	            if (trimStr.equals("][")) {
	            	continue;
	            }
	            
				if (trimStr.startsWith("}")) {
					brace_count--;
				} 
				else if (trimStr.endsWith("{")) {
					brace_count++;
				}         
	            
	            jsonObj += str;
	            pos += newSize;
	            
	            // find a JSON object
	            if (trimStr.startsWith("}") && brace_count == 0) {
	            	// remove the characters after the last "}"
	            	int last = jsonObj.lastIndexOf("}");
	            	value.set(jsonObj.substring(0, last+1));	   
	            	return true;
	            }
			}
			
			return false;
		}
		
	}

}