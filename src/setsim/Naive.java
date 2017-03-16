package setsim;

import preprocessing.Preprocessing.MY_COUNTER;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.io.File;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Naive extends Configured implements Tool {

	public static final double t = 0.6;
	
	public enum MY_COUNTER {
		  NUMBER_OF_COMPARISON
		};
	
	public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Naive(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Naive");
      job.setJarByClass(Naive.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      //job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setNumReduceTasks(10);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      
      return 0;
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private static Integer max_key = 0;
		
	      // Retrieving number of lines
	      static{
	          String temp = new String();
	          try { 
		          Scanner scanner = new Scanner(new File("/home/cloudera/workspace/Setsim/Number_of_output_records"));
	        	  temp = scanner.nextLine();
	              max_key = new Integer(Integer.parseInt(temp));
		          scanner.close();
	          } catch (FileNotFoundException e) {
	        	  System.out.println("Number of lines not found");
	          }
	      }	
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			HashMap<Integer, String> lines = new HashMap<Integer, String>();	    	
			int current_key;
			String current_value;
			
			for (String line: value.toString().split("[\\r\\n]+")) {
      		  	current_key = Integer.parseInt(line.substring(0, line.indexOf(",") ));
      		  	current_value = line.substring(line.indexOf(",")+1, line.length());
      		  	lines.put(current_key, current_value);	
			}
			//System.out.println("keyset : " + lines.keySet());
			for (int i : lines.keySet()){
				for (int j=1 ; j<=max_key ; j=j+1){
					if (j<i){
				    	StringBuilder pair = new StringBuilder();  
						pair.append(i);
						pair.append("$");
						pair.append(j);
						System.out.println(pair.toString()+lines.get(i));
						context.write(new Text(pair.toString()), new Text(lines.get(i)));
					}
					else if (j>i){
				    	StringBuilder pair = new StringBuilder();  
						pair.append(j);
						pair.append("$");
						pair.append(i);
						System.out.println(pair.toString()+lines.get(i));
						context.write(new Text(pair.toString()), new Text(lines.get(i)));
					}	
				}
			}
			
		}
	}

   public static class Reduce extends Reducer<Text, Text, Text, Double> {
	  
	  public static double similarity_calculation(String a, String b){
		  HashSet<String> s1 = new HashSet<String>();
		  HashSet<String> s2 = new HashSet<String>();
		  HashSet<String> union = new HashSet<String>();
		  int intersection = 0;
		  
		  for (String word: a.toString().split("\\s+")) {
			  s1.add(word);
			  union.add(word);
		  }
		  for (String word: b.toString().split("\\s+")) {
			  s2.add(word);
			  union.add(word);
		  }
		  for (String word: union){
			  if (s1.contains(word) && s2.contains(word)){
				  intersection++;
			  }
		  }
		  
		  return (double)intersection/(double)union.size();
	  }
	   
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 
   	   	 ArrayList<String> lines = new ArrayList<String>();
    	 //System.out.println("New reducer");
    	 //System.out.println("key : "+ key);
    	 for (Text value : values){
    		 lines.add(value.toString());
    		 //System.out.println(value.toString());
    	 }
    	 // Check if we have too many sentences or not
    	 if (lines.size() > 2){
    		 //System.out.println(lines.size());
    		 throw new RuntimeException("Error: comparison between more than two sentences");
    	 }
    	 if (lines.size() == 2){
	    	 // Similarity calculation
	    	 double sim = similarity_calculation(lines.get(0), lines.get(1));
	    	// Counter for number of comparison
	       	 context.getCounter(MY_COUNTER.NUMBER_OF_COMPARISON).increment(1);   
	         if (sim>t){
	    	 	context.write(key, sim);
	         }
    	 }

      }
   }
}
