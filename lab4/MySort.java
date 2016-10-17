/**
 ***************************************** 
 ***************************************** 
 * Cpr E 419 - Lab 4 *********************
 ***************************************** 
 ***************************************** 
 */

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySort extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new MySort(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {		
		    throw new IllegalArgumentException("\nargs format: input_path output_path temp_path partitionlist_path");
		}
		
		int reduce_tasks = 15;
		
		
		// based on the input file (last 4 chars), calculate the number of samples
		double freq = 0.05;
		int numSamples = 1000000;
		String inputStr = args[0];
		int length = inputStr.length();
		String numRecords = inputStr.substring(length - 4); 
		
		if (numRecords.equals("250M")) {
			numSamples = (int) (250 * 1000 * 1000 * freq);
			if (numSamples > 1000000) {
				numSamples = 1000000;
				freq = (double) numSamples / (250.0 * 1000 * 1000);
			}
		}
		else if (numRecords.equals("500K")) {
			numSamples = (int) (500 * 1000 * freq);
		}
		else if (numRecords.equals("-50K")) {
			numSamples = (int) (50 * 1000 * freq);
		}
		else if (numRecords.equals("-50M")) {
			numSamples = (int) (50 * 1000 * 1000 * freq);
			if (numSamples > 1000000) {
				numSamples = 1000000;
				freq = (double) numSamples / (50.0 * 1000 * 1000);
			}
		}
		else if (numRecords.equals("t-5K")) {
			numSamples = (int) (5 * 1000 * freq);						
		}
		else if (numRecords.equals("t-5M")) {
			numSamples = (int) (5 * 1000 * 1000 * freq);						
		}
		
		
		// set the first job
		Configuration conf = new Configuration();
		Job job_one = new Job(conf, "Lab4: Large Dataset Sort (Phase One)");
		job_one.setJarByClass(MySort.class);

		job_one.setNumReduceTasks(0);
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(CleanerMapper.class);

		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_one, new Path(args[2]));

		job_one.waitForCompletion(true);
		
		
		// set the second job
		Job job_two = new Job(conf, "Lab4: Large Dataset Sort (Phase Two)");
		job_two.setJarByClass(MySort.class);
				
		FileInputFormat.addInputPath(job_two, new Path(args[2]));
		FileOutputFormat.setOutputPath(job_two, new Path(args[1]));
		
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);
		job_two.setInputFormatClass(SequenceFileInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		//job_two.setMapperClass(Map.class);
		job_two.setReducerClass(Reduce.class);
		job_two.setNumReduceTasks(reduce_tasks);
		
		job_two.setPartitionerClass(TotalOrderPartitioner.class);
		// path for the sample file
		Path p = new Path(args[3]);
		TotalOrderPartitioner.setPartitionFile(job_two.getConfiguration(), p);
		
		
		// InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler<Text, Text>(numSamples);
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(freq, numSamples, 20);
		// InputSampler.Sampler<Text, Text> sampler = new InputSampler.IntervalSampler<Text, Text>(freq);
		InputSampler.writePartitionFile(job_two, sampler);
		
		// Add partitionFile to DistributedCache
		Configuration jobconf = job_two.getConfiguration();
		String partitionFile =TotalOrderPartitioner.getPartitionFile(jobconf);
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, jobconf);
		DistributedCache.createSymlink(conf);	
		
		// Run the job
		job_two.waitForCompletion(true);		

		return 0;
	}

	
	// The CleanerMapper Class
	public static class CleanerMapper extends Mapper<LongWritable, Text, Text, Text> {

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			if (line != null) {
				String keystr = line.substring(0, 10);
				String valuestr = line.substring(10);
				context.write(new Text(keystr), new Text(valuestr));
			}
		}
	}
	
	

//	// The Map Class
//	public static class Map extends Mapper<Text, Text, Text, Text> {
//
//		// The map method
//		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//
//			context.write(key, value);
//		}
//	}

	// The reduce class
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		// The reduce method
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text val : values) {

				context.write(key, val);
			}

		}

	}

}

