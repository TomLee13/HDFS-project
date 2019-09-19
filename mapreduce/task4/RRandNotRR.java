// ======================= WordCount.java ==========================================
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


public class RRandNotRR extends Configured implements Tool {

        public static class RRandNotRRMap extends Mapper<LongWritable, Text, Text, IntWritable>
        {
                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();
                
                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        String line = value.toString();
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        while(tokenizer.hasMoreTokens())
                        {
                                // get the next token
				String next = tokenizer.nextToken();
				System.out.println(next);
				// if we find "rr" in the current word
				if (randr(next))
				{
					word.set("RR");
                                	context.write(word, one);
				} else { // if we do not find "rr" in the current word
					word.set("notRR");
					context.write(word, one);
				}
                        }
                }

		// a private method in RandrMap class that aims to determine whether a word
		// contains "rr" or not.
		private boolean randr(String token) 
		{
			// loop from the start to the second last letter in the given token
			for (int i = 0; i < token.length() - 1; i++)
			{
				// if there is "rr" found
				if (token.substring(i, i + 2).equals("rr"))
				{
					return true;
				}
			}
			// if there is no "rr" found
			return false;
		}
        }
        
        public static class RRandNotRRReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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
                job.setJarByClass(RRandNotRR.class);
                job.setJobName("rrandnotrr");
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                job.setMapperClass(RRandNotRRMap.class);
                job.setCombinerClass(RRandNotRRReducer.class);
                job.setReducerClass(RRandNotRRReducer.class);
                
                
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                
                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }
        
       
        public static void main(String[] args) throws Exception {
                // TODO Auto-generated method stub
                int result = ToolRunner.run(new RRandNotRR(), args);
                System.exit(result);
        }
       
} 
