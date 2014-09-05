package de.mpii.fsm.stats;

import java.io.IOException;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import de.mpii.fsm.util.IntArrayWritable;


public class StatsCollector {

  /**
   * This class contains a number of jobs for collecting statistics from a set of input 
   * sequences including distribution of their lengths and distribution of distinct items
   * contained in each sequence
   *
   * @author miliaraki
   */

  static final Logger logger = Logger.getLogger(StatsCollector.class.getSimpleName());

  /**
   * Mapper reads (identifier,sequence) pairs and emits (sequence length, 0) pairs
   * 
   * @param key sequence identifier
   * @param value sequence
   * 
   */
  static class MeasureLengthMapper extends Mapper<LongWritable, IntArrayWritable, IntWritable, IntWritable> {

    IntWritable lengthWrit = new IntWritable();

    IntWritable countWrit = new IntWritable(1);

    protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {

      int[] sentenceArray = value.getContents();
      lengthWrit.set(sentenceArray.length);
      context.write(lengthWrit, countWrit);
    }
  }

  /**
   * The reducer sums up the counts for the different sequence lengths
   * 
   * @param key sequence length
   * @param value count
   * 
   */

  static class MeasureLengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    IntWritable lengthWrit = new IntWritable();

    IntWritable sumWrit = new IntWritable(0);

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable count : values) {
        sum += count.get();
      }

      lengthWrit.set(key.get());
      sumWrit.set(sum);

      context.write(lengthWrit, sumWrit);
    }
  }

  /**
   * Mapper reads (identifier,sequence) pairs and emits (sequence length, 0) pairs
   * 
   * @param key sequence identifier
   * @param value sequence
   * 
   */
  static class MeasureDistinctItemsMapper extends Mapper<LongWritable, IntArrayWritable, IntWritable, IntWritable> {

    IntWritable distinctWrit = new IntWritable();

    IntWritable countWrit = new IntWritable(1);

    protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {

      int[] sentenceArray = value.getContents();
      HashSet<Integer> items = new HashSet<Integer>();
      
      for (int item : sentenceArray) {
        items.add(item);
      }
      
      distinctWrit.set(items.size());
      context.write(distinctWrit, countWrit);
    }
  }

  /**
   * The reducer sums up the counts for the different distinct item counts
   * 
   * @param key distinct items
   * @param value count
   * 
   */

  static class MeasureDistinctItemsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    IntWritable distinct = new IntWritable();

    IntWritable sum = new IntWritable(0);

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sumInt = 0;

      for (IntWritable count : values) {
        sumInt += count.get();
      }

      distinct.set(key.get());
      sum.set(sumInt);

      context.write(distinct, sum);
    }
  }

  public static void main(String[] args) throws Exception {

    logger.setLevel(Level.INFO);
    // logger.setLevel(Level.OFF);

    if (args.length != 3) {
      logger.log(Level.WARNING, "Usage: StatsCollector <input path> <output path 1> <output path 2>");
      System.exit(-1);
    }
    
    // Create a new Job for measuring sequence length distribution
    Job job1 = new Job();
    
    // Specify various job-specific parameters  
    job1.setJarByClass(StatsCollector.class);
    job1.setJobName("StatsCollector");

    // set input and output paths
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    // set input and output format
    job1.setInputFormatClass(SequenceFileInputFormat.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    // set mapper and reducer class
    job1.setMapperClass(MeasureLengthMapper.class);
    job1.setReducerClass(MeasureLengthReducer.class);
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    // set number of reducers
    job1.setNumReduceTasks(10);

    // set output (key, value) pairs
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);

    // run job and wait to finish
    job1.waitForCompletion(true);

    // Create a new Job for measuring distinct items per sequence
    Job job2 = new Job();
    
    // Specify various job-specific parameters  
    job2.setJarByClass(StatsCollector.class);
    job2.setJobName("StatsCollector");

    // set input and output paths
    FileInputFormat.setInputPaths(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    // set input and output format
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    // set mapper and reducer class
    job2.setMapperClass(MeasureDistinctItemsMapper.class);
    job2.setReducerClass(MeasureDistinctItemsReducer.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(IntWritable.class);
    
    // set number of reducers
    job2.setNumReduceTasks(10);

    // set output (key, value) pairs
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(IntWritable.class);

    // run job and wait to finish
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }

}
