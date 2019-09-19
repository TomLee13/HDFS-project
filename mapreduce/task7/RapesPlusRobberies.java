// ======================= RapesPlusRoberries.java ==========================================
package org.myorg;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


// Author: Mingyang Li
public class RapesPlusRobberies extends Configured implements Tool {

        public static class RapesPlusRobberiesMap extends Mapper<LongWritable, Text, Text, IntWritable>
        {
                private final static IntWritable one = new IntWritable(1);
                private Text target = new Text(); // to be written to the context if targets are found
                
                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        // Get a line fromt input
			String line = value.toString();
			// Get the data of this record of crime in every column and store them in an array
			String[] crime = line.split("\t");
			// If we found "RAPE" or "ROBBERIES" in the current crime
			if (crime[4].equals("RAPE") || crime[4].equals("ROBBERY"))
			{
				// Write this record to the context
				target.set("RapesPlusRobberies");
				context.write(target, one);
			}
			
                        /*StringTokenizer tokenizer = new StringTokenizer(line);
                        while(tokenizer.hasMoreTokens())
                        {
                                // get the next token
				String next = tokenizer.nextToken();
				// get the data of this record of crime in every column and store them in an array
				String[] crime = next.split("\t");
				// System.out.println(next);
				// if we find "RAPE" and "ROBBERY" in the current word
				if (crime[4].equals("RAPE") || crime[4].equals("ROBBERY"))
				{
					target.set("RapesPlusRobbeies");
                                	context.write(target, one);
				}
                        }*/
                }
        }
        
        public static class RapesPlusRobberiesReducer extends Reducer<Text, IntWritable, Text, IntWritable>
        {
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
                {
                        int sum = 0;
                        for(IntWritable value: values)
                        {
                                sum += value.get();
                        }
                        context.write(key, new IntWritable(sum));
                }
                
        }
        
        public int run(String[] args) throws Exception  {
               
                Job job = new Job(getConf());
                job.setJarByClass(RapesPlusRobberies.class);
                job.setJobName("rapesplusrobberies");
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                job.setMapperClass(RapesPlusRobberiesMap.class);
                job.setCombinerClass(RapesPlusRobberiesReducer.class);
                job.setReducerClass(RapesPlusRobberiesReducer.class);
                
                
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                
                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }
        
       
        public static void main(String[] args) throws Exception {
                // TODO Auto-generated method stub
                int result = ToolRunner.run(new RapesPlusRobberies(), args);
                System.exit(result);
        }
       
} 
