// ======================= OaklandCrimeStatsKML.java ==========================================
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

public class OaklandCrimeStatsKML extends Configured implements Tool {
        // Global constant (X, Y) for the coordinates of 3803 Forbes Avenue in Oakland 
        private static final double X = 1354326.897; 
        private static final double Y = 411447.7828;

        public static class OaklandCrimeStatsKMLMap extends Mapper<LongWritable, Text, Text, Text>
        {
                private final static IntWritable one = new IntWritable(1);
                private Text nominal = new Text();
                private Text target = new Text(); // to be written to the context if targets are found
                
                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {       
                        nominal.set(""); // Set the nominal text to empty
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
                                        String lonLat = crime[8] + "," + crime[7] + "," + "0.000000";
                                        // Write this record to the context
				                        target.set(lonLat);
				                        context.write(nominal, target);
                                }
				
			            }
                        
			
                }
        }
        
        public static class OaklandCrimeStatsKMLReducer extends Reducer<Text, Text, Text, Text>
        {       
                private Text output = new Text();
                private StringBuilder result = new StringBuilder();

                /*@Override
                protected void setup(Context context) throws IOException, InterruptedException {
                    String kmlStart = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                        + "<kml xmlns=\"http://earth.google.com/kml/2.2\">\n" + "<Document>\n" + "<Style id=\"style1\">\n"
                        + "<IconStyle>\n" + "<Icon>\n"
                        + "<href>http://maps.gstatic.com/intl/en_ALL/mapfiles/ms/micons/blue-dot.png</href>\n" + "</Icon>\n"
                        + "</IconStyle>\n" + "</Style>\n";
                    result.append(kmlStart);
                }

                @Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
                    String kmlEnd = "</Document>\n" + "</kml>\n";
                    result.append(kmlEnd);
                }*/

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                        //int sum = 0;
                        String kmlStart = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                            + "<kml xmlns=\"http://earth.google.com/kml/2.2\">\n" + "<Document>\n" + "<Style id=\"style1\">\n"
                            + "<IconStyle>\n" + "<Icon>\n"
                            + "<href>http://maps.gstatic.com/intl/en_ALL/mapfiles/ms/micons/blue-dot.png</href>\n" + "</Icon>\n"
                            + "</IconStyle>\n" + "</Style>\n";
                        result.append(kmlStart);
                        for(Text value: values)
                        {
                                //sum += value.get();
                                result.append(constructKml(value));
                                //output.set(constructKml(value));
                                //context.write(output, null);
                        }
                        String kmlEnd = "</Document>\n" + "</kml>\n";
                        result.append(kmlEnd);
                        output.set(result.toString());
                        System.out.println(result.toString());
                        context.write(output, key);
                        //context.write(key, new IntWritable(sum));
                }

                private static String constructKml(Text value) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<Placemark>\n").append("<name>" + "crime" + "</name>\n")
                        .append("<description>" + "crime" + "</description>\n")
                        .append("<styleUrl>#style1</styleUrl>\n")
                        .append("<Point>\n")
                        .append("<coordinates>" + value.toString() + "</coordinates>\n")
                        .append("</Point>\n" + "</Placemark>\n");                   
                    return sb.toString();
                  }
                
        }
        
        public int run(String[] args) throws Exception  {
               
                Job job = new Job(getConf());
                job.setJarByClass(OaklandCrimeStatsKML.class);
                job.setJobName("oaklandcrimestatskml");
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setMapperClass(OaklandCrimeStatsKMLMap.class);
                //job.setCombinerClass(OaklandCrimeStatsKMLReducer.class);
                job.setReducerClass(OaklandCrimeStatsKMLReducer.class);
                
                
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                
                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }
        
       
        public static void main(String[] args) throws Exception {
                // TODO Auto-generated method stub
                int result = ToolRunner.run(new OaklandCrimeStatsKML(), args);
                System.exit(result);
        }
       
} 
