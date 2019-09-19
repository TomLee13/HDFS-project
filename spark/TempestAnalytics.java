package edu.cmu.andrew.student061.project5spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

// Author: Mingyang Li
public class TempestAnalytics {
    
    /**
     * A method that uses JavaRDD.count to count the total number of lines in the input file
     * @param fileName the name of the input file
     */
    private static void numOfLines(JavaRDD<String> input) {
        // Gets the number of entries in the RDD
        long count = input.count();
        System.out.println(String.format("Total lines is %d", count));
    }
    
    /**
     * A method that uses JavaRDD.flatMap() to count the number of words
     * @param input A JavaRDD input
     */
    private static void numOfWords(JavaRDD<String> input) {
        // Identify words in the file (split with whitespace and punctuations)
        JavaRDD<String> wordsFromFile = input.flatMap(content -> Arrays.asList(content.split("\\W+")));
        // Get the number of words
        long count = wordsFromFile.count();
        System.out.println(String.format("Total words is %d", count));
    }
    
    /**
     * A method that uses JavaRDD.distinct() method to count the number of distinct words
     * @param input A JavaRDD input
     */
    private static void numOfDistinctWords(JavaRDD<String> input) {
        // Identify words in the file (split with whitespace and punctuations)
        JavaRDD<String> wordsFromFile = input.flatMap(content -> Arrays.asList(content.split("\\W+")));
        // Find the distinct words in the file
        JavaRDD<String> distinctWords = wordsFromFile.distinct();
        // Get the number of distinct words
        long count = distinctWords.count();
        System.out.println(String.format("Total distinct words is %d", count));
    }
    
    /**
     * A method that fulfills the requirements of task3
     * @param input A JavaRDD input
     */
    private static void numOfWordsFile1(JavaRDD<String> input) {
        // Identify words in the file (split with whitespace and punctuations)
        JavaRDD<String> wordsFromFile = input.flatMap(content -> Arrays.asList(content.split("\\W+")));
        // Use mapToPair() method to show every word with a digit 1
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1));
        // Save the output to a text file
        countData.saveAsTextFile("TheTempestOutputDir1");
        System.out.println("file saved");
    }
    
    /**
     * A method that fulfills the requirements of task4
     * @param input A JavaRDD input
     */
    private static void numOfWordsFile2(JavaRDD<String> input) {
        // Identify words in the file (split with whitespace and punctuations)
        JavaRDD<String> wordsFromFile = input.flatMap(content -> Arrays.asList(content.split("\\W+")));
        // Use mapToPair() method to show every word with a digit 1
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
        // Save the output to a text file
        countData.saveAsTextFile("TheTempestOutputDir2");
        System.out.println("file saved");
    }
    
    /**
     * A method that fulfills the requirements of task5
     * @param input A JavaRDD input
     */
    private static void searchByLine(JavaRDD<String> input) {
        // Ask for user input
        System.out.println("Please enter a word to search:");
        Scanner scan = new Scanner(System.in);
        String searchWord = scan.nextLine();
        // Perform search
        if (searchWord == null || searchWord.equals("")) {
            System.out.println("Invalid input");
        } else {
            // for each line in the input JavaRDD
            input.foreach(line -> {
                String[] words = line.split("\\W+"); // Split the line with whitespace and punctuations
                boolean found = false;
                // Search through the words in this line
                for (String word : words) {
                    if (word.toLowerCase().equals(searchWord.toLowerCase())) {
                        found = true;
                    }
                }
                if (found) { // if the word is found
                    // Print the whole line
                    System.out.println(line);
                }
            });
            System.out.println("The lines above contains the search word");
        }
        
    }
    
    /**
     * A method that will be called in the main method that execute all the tasks
     * @param fileName A String of the file name
     */
    private static void execute(String fileName) {
        // Code from the lab
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("File Copy");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // Read the source file
        JavaRDD<String> input = sparkContext.textFile(fileName);
        
        numOfLines(input);
        numOfWords(input);
        numOfDistinctWords(input);
        numOfWordsFile1(input);
        numOfWordsFile2(input);
        searchByLine(input);
    }
    
    public static void main(String[] args) {
        
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        execute(args[0]);
    }
}
