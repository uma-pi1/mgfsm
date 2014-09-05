package de.mpii.fsm.output;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.mpii.fsm.util.Dictionary;
import de.mpii.fsm.util.IntArrayWritable;


/**
 * This class translates a sequence of item identifiers into a sequence of item
 * names using an input dictionary.
 * 
 * @author Iris Miliaraki
 * 
 */

public class SequenceTranslator {

	static final Logger logger = Logger.getLogger(SequenceTranslator.class
			.getSimpleName());
	
	/**
	 * Translates input sequences of term identifiers
	 * 
	 * @author Iris Miliaraki
	 * @author Kaustubh Beedkar (kbeedkar@mpi-inf.mpg.de)
	 *
	 */

	static class SequenceTranslatorMapper extends Mapper<IntArrayWritable, LongWritable, Text, LongWritable> {

		StringBuffer itemNameSequence = new StringBuffer();
		Text itemNameSequenceTxt = new Text();

		// Map each word to its partition
		Map<Integer, String> itemIdToName = new HashMap<Integer, String>();

		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			// Read dictionary
			try {
				ObjectInputStream is = new ObjectInputStream(new FileInputStream("itemIdToName"));
				itemIdToName = (Map<Integer, String>) is.readObject();
				is.close();
			} catch (ClassNotFoundException e) {
				logger.severe("Mapper setup - Exception during deserialization: " + e);

			}
		}

		public void map(IntArrayWritable key, LongWritable value, Context context) throws IOException, InterruptedException {

			int[] itemIdsequence = key.getContents();

			itemNameSequence = new StringBuffer();
			
			for (int itemId : itemIdsequence) {
				String itemName = itemIdToName.get(itemId);
				itemNameSequence.append(itemName + " ");
			}
			itemNameSequenceTxt.set(itemNameSequence.toString().trim());
			context.write(itemNameSequenceTxt, value);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
						
			//itemIdToName.clear(); //clean up

		}
	}

	public static void main(String[] args) throws Exception {

		logger.setLevel(Level.INFO);
		
	    if (args.length != 3) {
	      logger.log(Level.WARNING, "Usage: SequenceTranslator <input path> <output path> <item dictionary path>");
	      System.exit(-1);
	    }

		Job job = new Job();
		job.setJarByClass(SequenceTranslator.class);
		job.setJobName("Sequence translator");

		job.getConfiguration().set("dictionary", args[2]);

		

		Configuration conf = job.getConfiguration();
		// Allow usage of symbolic links for the distributed cache
		DistributedCache.createSymlink(conf);

		String dictionaryURI = conf.get("dictionary");
		
		// Map each item id to its name
		Map<Integer, String> itemIdToName = (new Dictionary().readDictionary(conf, dictionaryURI));
				
		// Serialize itemIdToName, each mapper reads it from DCache

		try {

			// Write the object to a temporary HDFS location
			String itemIdToNameInfoObject = "itemIdToName";
			FileSystem fs = FileSystem.get(URI.create(itemIdToNameInfoObject), conf);
			ObjectOutputStream os = new ObjectOutputStream(fs.create(new Path(
					itemIdToNameInfoObject)));
			os.writeObject(itemIdToName);
			os.close();

			DistributedCache.addCacheFile(new URI(itemIdToNameInfoObject + "#itemIdToName"), conf);

		} catch (Exception e) {
			logger.severe("Exception during serialization: " + e);
		}

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SequenceTranslatorMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		

		job.waitForCompletion(true);	


	}

}
