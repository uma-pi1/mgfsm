package de.mpii.fsm.input;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;

import de.mpii.fsm.util.IntArrayWritable;


/**
 * This class uploads to HDFS in the agreed data format the test files that we have on our wiki
 * 
 * Usage:
 * hadoop jar fsm.jar org.apache.mahout.fsm.InputFileConverter localFile hdfsFile
 * e.g.
 * hadoop jar fsm.jar org.apache.mahout.fsm.InputFileConverter /home/szoup/test/input1 /user/szoup/test
 * 
 * @author Spyros Zoupanos
 */
public class InputFileConverter {

  public static void main(String[] args) throws IOException {
    FileInputStream fstream = new FileInputStream(args[0]);
    // Get the object of DataInputStream
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
    SequenceFile.Writer fileWriter = new SequenceFile.Writer(fs, conf, new Path(args[1]), LongWritable.class, IntArrayWritable.class);
    
    String strLine;
    long counter = 0;
    //Read File Line By Line
    while ((strLine = br.readLine()) != null)  {
      String[] strTerms = strLine.split("\t");
      
      int[] intTerms = new int[strTerms.length];
      for(int i=0; i<strTerms.length; i++)
        intTerms[i] = Integer.parseInt(strTerms[i]);
      fileWriter.append(new LongWritable(counter), new IntArrayWritable(intTerms));
      counter++;
      System.out.println (intTerms);
    }
    //Close the input stream
    in.close();
    fileWriter.close();
  }
}
