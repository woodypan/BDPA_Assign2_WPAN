package preprocessing;

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

public class Preprocessing extends Configured implements Tool {

	public enum MY_COUNTER {
		  NUMBER_OF_LINES
		};
	
	public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Preprocessing(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Preprocessing");
      job.setJarByClass(Preprocessing.class);
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
      
      // Code from stackoverflow to write a file on HDFS
      FileSystem fs = FileSystem.get(job.getConfiguration()); 
      Path filenamePath = new Path("Number_of_output_ecords");  
      try {
          if (fs.exists(filenamePath)) {
              fs.delete(filenamePath, true);
          }

          FSDataOutputStream fin = fs.create(filenamePath);
          long c = job.getCounters().findCounter(MY_COUNTER.NUMBER_OF_LINES).getValue();
          fin.writeUTF(String.valueOf(c));
          fin.close();
      } catch(IOException e) {System.out.println("IO Error");}
      
      return 0;
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Integer file = 1;

	      static List<String> stopword = new ArrayList<String>();

	      // Retrieving stopword list
	      static{
	          String temp = new String();
	          try { 
		          Scanner scanner = new Scanner(new File("/home/cloudera/workspace/MyInvertedIndex/StopWords.csv"));
		          while(scanner.hasNextLine()){
		        	  temp = scanner.nextLine();
		              stopword.add(temp.substring(0, temp.indexOf(",")));
		          }
		          scanner.close();
	          } catch (FileNotFoundException e) {
	        	  System.out.println("Stopwords file not found");
	          }
	      }		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Filtering out empty lines, special characters and stopwords
			for (String token0: value.toString().split("[\\r\\n]+")) {
				for (String token: token0.toString().split("\\s+")) {
		    		token = token.replaceAll("[^a-zA-Z0-9]","");
		    		token = token.toLowerCase();
		    		if(!stopword.contains(token))
		            {
		                word.set(token);
						context.write(new Text(file.toString()), word);
		            }
	
		      	}
				file = new Integer(file.intValue() + 1);
			}
	    	 
			
		}
	}

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
 
	  // Read the frequencies_pg100.txt file and parse it
      static HashMap<String, Integer> frequencies = new HashMap<String, Integer>();
      static{
     	 String strval = new String();
     	 int countval;
          try { 
	          Scanner scanner = new Scanner(new File("/home/cloudera/workspace/Setsim/frequencies_pg100.txt"));
	          while(scanner.hasNextLine()){
	        	  strval = scanner.nextLine();
        		  countval = Integer.parseInt(strval.substring(strval.indexOf(",")+1, strval.length()));
        		  strval = strval.substring(0, strval.indexOf(","));
	              frequencies.put(strval, countval);
	          }
	          scanner.close();
          } catch (FileNotFoundException e) {
        	  System.out.println("Frequencies file not found");
          }
      }	 
	   
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
 
    	 // Retrieve words of a document and remove duplicates
    	 HashMap<String, Integer> words = new HashMap<String, Integer>();
         for (Text val : values) {
        	 String strval = val.toString();
        	 
        	 if (!words.containsKey(strval)){
        		 words.put(strval, frequencies.get(strval));
        	 }
        	 
         }
         // Sorting by ascending order of frequency
         ArrayList<String> words_list = new ArrayList<String>();
         while (words.size()>0){
        	 String minfreqword = words.keySet().iterator().next();
        	 int minfreq = frequencies.get(minfreqword);
        	 for (String temp : words.keySet()){
        		 if (minfreq > words.get(temp)){
        			 minfreq = words.get(temp);
        			 minfreqword = temp;
        		 }
        	 }
        	 words_list.add(minfreqword);
        	 words.remove(minfreqword);
         }
         
         // Rebuilt the output string from words_list
    	 StringBuilder sentence = new StringBuilder();         
         for (String word : words_list) {
        	 if (word.length() != 0) {
        		 sentence.append(word + " ");
        	 }
         }
         if (sentence.length()>1) {
         	sentence.setLength(sentence.length() - 1);
            // Counter for number of lines
           	 context.getCounter(MY_COUNTER.NUMBER_OF_LINES).increment(1);          
         }

         context.write(key, new Text(sentence.toString()));

      }
   }
}
