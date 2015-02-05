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
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
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
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
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
    
    Path path = new Path(fs.getUri());
        
    LongWritable itemKey = new LongWritable();
    IntArrayWritable itemValue = new IntArrayWritable();
    
    
    /** GzipCodec might not work */
    CompressionCodec Codec = new GzipCodec();
    
    Option optPath = SequenceFile.Writer.file(path);
    Option optKey = SequenceFile.Writer.keyClass(itemKey.getClass());
    Option optValue = SequenceFile.Writer.valueClass(itemValue.getClass());
    Option optCom = SequenceFile.Writer.compression(CompressionType.RECORD, Codec);
    
    SequenceFile.Writer fileWriter = SequenceFile.createWriter(conf, optPath, optKey, optValue, optCom);
    
    
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
