import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//Lab 11: Data-Driven Advertising

public class Advertising extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Advertising(), args);
		System.exit(res); 
	}
	
	public int run ( String[] args ) throws Exception {    
		Configuration conf = new Configuration();
		// Number of reducers
		int reduce_tasks = 8;
		
		/* job_one: create the recipient folder
		 * line: user    recipient list | price | total | rate (price/total)
		 * example: 9       4,3,6,1,2,7,8,5,|price:65.25100080849963|total:8|rate:8.156375101062453
		 * */
		Job job_one = new Job(conf, "lab_11 round one"); 
		job_one.setJarByClass(Advertising.class); 
        
		job_one.setNumReduceTasks(reduce_tasks);
        
        // Set the Output Key and Value from the reducer
		job_one.setOutputKeyClass(IntWritable.class);        
		job_one.setOutputValueClass(Text.class);
        
        // Set the Map and Reducer class
		job_one.setMapperClass(Map_One.class);             
		job_one.setReducerClass(Reduce_One.class);
        
        // Input and Output format
		job_one.setInputFormatClass(TextInputFormat.class); 
		job_one.setOutputFormatClass(TextOutputFormat.class);
        
        // Input and Output path	      
		FileInputFormat.addInputPath(job_one, new Path("/class/s14419x/lab11/log.txt"));
		FileInputFormat.addInputPath(job_one, new Path("/class/s14419x/lab11/compensation.txt"));
		FileOutputFormat.setOutputPath(job_one, new Path("/scr/yuz1988/lab11/recipient"));
        
        // Run the job
		job_one.waitForCompletion(true);
		
		
		/* job_two: sorting by the rate
		 * line: user   recipient list | price | total | rate (price/total)
		 * example: 9       4,3,6,1,2,7,8,5,|price:65.25100080849963|total:8|rate:8.156375101062453
		 * */
		Job job_two = new Job(conf, "lab_11 round two"); 
		job_two.setJarByClass(Advertising.class); 
        		
		// map all the input to one reducer 
		job_two.setNumReduceTasks(1);
        
        // Set the Output Key and Value from the reducer
		job_two.setOutputKeyClass(IntWritable.class);        
		job_two.setOutputValueClass(Text.class);
        
        // Set the Map and Reducer class
		job_two.setMapperClass(Map_Two.class);             
		job_two.setReducerClass(Reduce_Two.class);
        
        // Input and Output format
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
        
        // Input and Output path	      
		FileInputFormat.addInputPath(job_two, new Path("/scr/yuz1988/lab11/recipient"));
		FileOutputFormat.setOutputPath(job_two, new Path("/scr/yuz1988/lab11/rate"));
        
        // Run the job
		job_two.waitForCompletion(true);		
		
		return 0;
	} 
	
	
	/*********************************Round One*************************************/
    // The Map_One Class 
	public static class Map_One extends Mapper<LongWritable, Text, IntWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {
			
			String line = value.toString();                               
			String[] tokens = line.split(",");
			// compensation.txt
			if (tokens.length == 2) {
				int user = Integer.parseInt(tokens[0]);
				String str = "price:" + tokens[1];
				context.write(new IntWritable(user), new Text(str));
			}
			// log.txt
			else {
				tokens = line.split("\\.");
				String[] tokens1 = tokens[0].split("\\s+");
				int user = Integer.parseInt(tokens1[1]);
				String str = "re:" + tokens1[4];
				context.write(new IntWritable(user), new Text(str));
			}
		} 	
	} 
	
    // The Reduce_One class
	public static class Reduce_One extends Reducer<IntWritable, Text, IntWritable, Text>  {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
								throws IOException, InterruptedException  {
			int total = 0;      // number of recipients
			double price = 0.0;
			double rate = 0.0;
			String info = "";   // recipient list | price | total | rate
			
			for (Text val : values) {
				String str = val.toString();
				if (str.startsWith("price:")) {
					price = Double.parseDouble(str.substring(6));
				}
				else if (str.startsWith("re:")) {
					info += str.substring(3) + ",";
					total++;
				}
			}
			
			if (total!=0 && price!=0.0) {
				rate = price / total;
				info += "|" + price + "|"  + total + "|" + rate;
				context.write(key, new Text(info));
			}
			
		} 
		
	}
	
	
	/*********************************Round Two*************************************/
	 // The Map_Two Class 
		public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text>  {
			 
			public void map(LongWritable key, Text value, Context context)
								throws IOException, InterruptedException  {
				
				context.write(new IntWritable(1), value);
				
			} 	
		} 
		
		// Comparator
		// Sort the string based on the value of rate
		public static class MyComparator implements Comparator<String> {		
			public int compare(String str1, String str2) {
				String[] tokens1 = str1.split("\\|");
				String[] tokens2 = str2.split("\\|");
				double rate1 = Double.parseDouble(tokens1[3]);
				double rate2 = Double.parseDouble(tokens2[3]);
				if (rate1 > rate2)
					return 1;
				else if (rate1 < rate2)
					return -1;
				else
					return 0;
			}
				 
		}
		
		public static class Reduce_Two extends Reducer<IntWritable, Text, Text, Text>  {
			
			private double budget = 10000;
			
	        // The reduce method
			public void reduce(IntWritable key, Iterable<Text> values, Context context) 
									throws IOException, InterruptedException  {	
				ArrayList<String> al = new ArrayList<String> ();
				for (Text val: values) {
					String str = val.toString().trim();
					al.add(str);			
				}
				// sort the list
				Collections.sort(al, new MyComparator());
				
				int users = 0;
				HashSet<String> hs = new HashSet<String>();
				for (String str : al) {
					String[] tokens = str.split("\\|");
					double price = Double.parseDouble(tokens[1]);
					int total = Integer.parseInt(tokens[2]);
					if ((budget-price) >= 0) {
						budget = budget - price;
						// tokens[0]: user   recipient_list
						// example: 9  4,3,8,5,
						String[] tokens1 = tokens[0].split("\\s+");
						String user = tokens1[0];
						String recipients = tokens1[1];
						
						// add user to the hashset
						hs.add(user);
						users++;
						// add recipients to the hashset
						String[] recipient_list = recipients.split(",");
						for (String str1 : recipient_list) {
							hs.add(str1);
						}
						String info = "price:"+price+" people:"+recipients+" total:"+total;
						context.write(new Text("user:"+user), new Text(info));
					}					
				}
				String consumers = "total number of consumers:" + hs.size();
				String budget_used = "total number of users:" + users + ", total budget used:" + (10000 - budget);
				context.write(new Text(consumers), new Text(budget_used));
			} 
			
		}		
	
}
