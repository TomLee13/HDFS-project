// ======================= AssaultInOakland.java ==========================================
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
public class AssaultInOakland extends Configured implements Tool {
        // Global constant (X, Y) for the coordinates of 3803 Forbes Avenue in Oakland 
        private static final double X = 1354326.897; 
        private static final double Y = 411447.7828;

        public static class AssaultInOaklandMap extends Mapper<LongWritable, Text, Text, IntWritable>
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
			            if (crime[4].equals("AGGRAVATED ASSAULT"))
			            {
                                // Get the x and y for this crime
                                double x = Double.parseDouble(crime[0]);
                                double y = Double.parseDouble(crime[1]);
                                // Compute the distance between the location of the crime and 3803 Forbes Ave
                                double dist = Math.sqrt((x - X) * (x - X) + (y - Y) * (y - Y));
                                if (dist * 0.3048 < 200) // If the distance is within 200 meters of 3803 Forbes Ave
                                {
                                        // Write this record to the context
				                    target.set("Record");
				                    context.write(target, one);
                                }
				
			            }
                        
			
                }
        }
        
        public static class AssaultInOaklandReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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
                job.setJarByClass(AssaultInOakland.class);
                job.setJobName("assaultinoakland");
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                job.setMapperClass(AssaultInOaklandMap.class);
                job.setCombinerClass(AssaultInOaklandReducer.class);
                job.setReducerClass(AssaultInOaklandReducer.class);
                
                
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                
                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }
        
       
        public static void main(String[] args) throws Exception {
                // TODO Auto-generated method stub
                int result = ToolRunner.run(new AssaultInOakland(), args);
                System.exit(result);
        }
       
} 
