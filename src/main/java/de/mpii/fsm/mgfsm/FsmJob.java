package de.mpii.fsm.mgfsm;


import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.mpii.fsm.driver.FsmConfig;
import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.util.Constants;
import de.mpii.fsm.util.Dictionary;
import de.mpii.fsm.util.IntArrayWritable;


/**
 * During job setup, item frequencies are read and items are assigned to partitions. Mappers read
 * (id, transaction) pairs and emit (partitionId+encodedTransaction, 1) pairs for each distinct
 * partition in the input transaction. Each transaction is encoded and potentially splitted into
 * subsequences using {@link de.mpii.fsm.mgfsm.encoders.BaseGapEncoder} class. If a transaction contains
 * multiple items of the same partition, the sentence is emitted only once per partition. Each
 * reducer receives (sequence, count) pairs and for each set of sequences of a single partition, an
 * FSM algorithm is executed to get the frequent ones.
 * 
 * This class considers as input the item frequencies and identifiers. The {@link DicReader} class
 * is used to read this input dictionary.
 * 
 * @author Iris Miliaraki
 * @author Spyros Zoupanos
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 */
public class FsmJob {
    
    /* Checklist */
    
    /**
     * TODO : Move these into config :
     *  
     * public static final long PARTITION_SIZE_DEFAULT = 10000;
     * public static long partitionSize = PARTITION_SIZE_DEFAULT;
     * public static boolean ALLOW_SPLITS;
     * public static boolean REMOVE_UNREACHABLE;
     * public static IndexingMethod INDEXING_METHOD;
     * 
     * STATUS : Done, need to do testing.
     */
  
    static final Logger LOGGER = Logger.getLogger(FsmJob.class.getSimpleName());
    /**
     * A common configuration object containing all the 
     * necessary parameters for executing a MG-FSM job.*/
    
    private static FsmConfig commonConfig = new FsmConfig();
   
    //GETTER & SETTER METHODS
    public static FsmConfig getCommonConfig() {
      return commonConfig;
    }

    public static void setCommonConfig(FsmConfig commonConfig) {
      FsmJob.commonConfig = commonConfig;
    }
    //END OF GETTER & SETTER METHODS
    
    /**
     * Main method for initializing and running a MapReduce job implementing PFSM algorithm with a
     * mapper {@link FsmMapper}, a combiner {@link FsmCombiner} and a reducer
     * {@link FsmReducer}. We initially assign items to partitions by reading our dictionary
     * using {@link org.apache.mahout.fsm.util.DicReader}.
     * 
     * @throws Exception
     */
    public static void runFsmJob() throws Exception {
        // TODO: should take FsmConfig instance
        LOGGER.setLevel(Level.INFO);
        // logger.setLevel(Level.OFF);


        // read execution parameters for FSM algorithm sigma, gamma, lambda
        int sigma = commonConfig.getSigma();
        int gamma = commonConfig.getGamma();
        int lambda = commonConfig.getLambda();
        //int outputSequenceType = commonConfig.getOutputSequenceType();
        Type outputType = commonConfig.getType();

        // specify a single partition to run against (default 0) -- for
        // debugging reasons
        int debugPartId = commonConfig.getDebugPartitionId();

        boolean useSequenceFiles = commonConfig.isUseSequenceFiles(); // by default true, false for FsmTester class
        int numberOfReducers = commonConfig.getNumberOfReducers(); // by default 90

        Job job = new Job();
        job.setJarByClass(FsmJob.class);
        job.setJobName("MG-FSM (" + sigma + "," + gamma + "," + lambda + "," + debugPartId + ")");

        // set timeout to 60 minutes
        job.getConfiguration().setInt("mapreduce.task.timeout", 3600000);

        // whether multiple instances of some map/reduce tasks may be executed
        // in parallel.
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);
        job.getConfiguration().set("dictionary", commonConfig.getDictionaryPath());

        // set parameters
        job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.sigma", sigma);
        job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.gamma", gamma);
        job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.lambda", lambda);
        //job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.outputSequenceType", outputSequenceType);
        job.getConfiguration().setEnum("org.apache.mahout.fsm.partitioning.outputType", outputType);

        job.getConfiguration().setBoolean("org.apache.mahout.fsm.partitioning.allowSplits", commonConfig.isAllowSplits());
        job.getConfiguration().setEnum("org.apache.mahout.fsm.partitioning.indexingMethod", commonConfig.getIndexingMethod());
        job.getConfiguration().setBoolean("org.apache.mahout.fsm.partitioning.removeUnreachable", commonConfig.isRemoveUnreachable());
        job.getConfiguration().setBoolean("org.apache.mahout.fsm.partitioning.compressGaps", commonConfig.isCompressGaps());
        
        job.getConfiguration().setBoolean("org.apache.mahout.fsm.partitioning.bufferTransactions", commonConfig.isBufferTransactions());

        // an index mapping partitions to their set of items
        // TODO: TreeSet is redundant, only a range [minId, maxId] is required,
        // can be packed in a
        // long
        Map<Integer, TreeSet<Integer>> partitionToItems = new HashMap<Integer, TreeSet<Integer>>();

        // an index mapping items to partitions
        Map<Integer, Integer> itemToPartition = new HashMap<Integer, Integer>();

        Configuration conf = job.getConfiguration();

        // allow usage of symbolic links for the distributed cache
        DistributedCache.createSymlink(conf);

        String dictionaryURI = conf.get("dictionary");
        Dictionary dicReader = new Dictionary();
        int[] colsToLoad = new int[2];
        colsToLoad[0] = Constants.DOC_FREQ;
        colsToLoad[1] = Constants.ITEM_ID;
        dicReader.load(job.getConfiguration(), dictionaryURI, colsToLoad, sigma, -1);

        // Sequence file for frequent 1 items
        String fListURI = "fList";
        //FileSystem fs1 = FileSystem.get(URI.create(fListURI), conf);
        Path fListPath = new Path(fListURI);
        
        commonConfig.setFlistPath(fListPath);
        
        IntArrayWritable itemKey = new IntArrayWritable();
        LongWritable itemValue = new LongWritable();
        
        //CompressionCodec Codec = new GzipCodec();
        SequenceFile.Writer writer = null;
        Option optPath = SequenceFile.Writer.file(fListPath);
        Option optKey = SequenceFile.Writer.keyClass(itemKey.getClass());
        Option optValue = SequenceFile.Writer.valueClass(itemValue.getClass());
        //Option optCom = SequenceFile.Writer.compression(CompressionType.RECORD, Codec);
        
        writer = SequenceFile.createWriter(conf, optPath, optKey, optValue);
        
        int totalPartitions = 0;
        long partitionCurrSize = 0;

        int[] itemIdList = dicReader.itemIds;
        
        //TODO: Find a better solution to this
        // If the support is high then the itemIdList is null, check this.
        // And exit  before performing any operation on itemIdList
        if(itemIdList==null){
          LOGGER.log(Level.INFO, "No frequent pattern found.");
          System.exit(1);
        }
        
          // sort item id list by frequency -- pre-ordered lexicographically
          if (itemIdList.length > 0) {
              Arrays.sort(itemIdList);
          }
        

        // create also item id list ordered by descending frequency
        int[] reverseItemIdList = new int[itemIdList.length];
        int i = 0;
        for (int j = itemIdList.length - 1; j >= 0; j--) {
            reverseItemIdList[i] = itemIdList[j];
            i++;
        }

        int pointer = 0;
        int itemsAddedReverse = itemIdList.length - 1;

        // create the partitions reading item ids in descending order of their
        // frequencies
        itemIdList = reverseItemIdList;

        while ((pointer <= itemIdList.length - 1) && (pointer <= itemsAddedReverse)) {

            Integer itemId = (Integer) itemIdList[pointer];

            Integer frequency = dicReader.docFreqs[dicReader.posOf(itemId)];
            pointer++;

            // ignore items of frequency < sigma and do not assign them to
            // partitions
            if (frequency < sigma) {
                continue;
            }

            // adding item and its frequency to the sequence file
            itemKey.setContents(new int[] { itemId });
            itemValue.set(frequency);
            writer.append(itemKey, itemValue);

            // update partition size if current item is added
            long partitionNewSize = partitionCurrSize + frequency;

            TreeSet<Integer> itemList = new TreeSet<Integer>();

            if (partitionNewSize <= commonConfig.getPartitionSize() && totalPartitions > 0) {

                // if partition not full yet, add current item
                itemList = partitionToItems.get(totalPartitions);
                itemList.add(itemId);

                itemToPartition.put(itemId, totalPartitions);
                partitionToItems.put(totalPartitions, itemList);
                partitionCurrSize += frequency;

            } else if (frequency <= commonConfig.getPartitionSize()) {

                // create new partition
                totalPartitions++;
                partitionCurrSize = frequency;
                itemList.add(itemId);

                itemToPartition.put(itemId, totalPartitions);
                partitionToItems.put(totalPartitions, itemList);

            } else if (frequency > commonConfig.getPartitionSize()) {

                // create a large partition containing one highly frequent item
                totalPartitions++;
                partitionCurrSize = frequency;
                itemList.add(itemId);

                itemToPartition.put(itemId, totalPartitions);
                partitionToItems.put(totalPartitions, itemList);
            }
        }
        IOUtils.closeStream(writer);

        LOGGER.info(itemToPartition.size() + " items mapped to " + totalPartitions + " partitions ");

        // set parameters
        job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.totalPartitions", totalPartitions);
        job.getConfiguration().setInt("org.apache.mahout.fsm.partitioning.debugPartId", debugPartId);

        /**
         * Serialize objects stopWordSet partitionToWordList and wordToPartition Each mapper/reducer
         * will access them through the distributed cache
         */
        try {

            String part2ItemInfoObject = "partitionToItems";
            FileSystem fs = FileSystem.get(URI.create(part2ItemInfoObject), conf);
            ObjectOutputStream os = new ObjectOutputStream(fs.create(new Path(part2ItemInfoObject)));

            os.writeObject(partitionToItems);
            os.close();
            DistributedCache.addCacheFile(new URI(part2ItemInfoObject + "#partitionToItems"), conf);

            // Mapping item identifiers to partitions
            String item2PartInfoObject = "itemToPartition";
            os = new ObjectOutputStream(fs.create(new Path(item2PartInfoObject)));
            os.writeObject(itemToPartition);
            os.close();

            DistributedCache.addCacheFile(new URI(item2PartInfoObject + "#itemToPartition"), conf);
        } catch (Exception e) {
            LOGGER.severe("Exception during serialization: " + e);
        }

        // job parameters
        
        FileInputFormat.setInputPaths(job, new Path(commonConfig.getInputPath()));
        if (outputType == Type.ALL)
            FileOutputFormat.setOutputPath(job, new Path(commonConfig.getOutputPath()));
        else
            FileOutputFormat.setOutputPath(job, new Path(commonConfig.getTmpPath()));

        // define a custom comparator for sorting keys and a custom partitioner

        if (commonConfig.isUseAggregation())
            job.setSortComparatorClass(FsmRawComparator.class);

        job.setPartitionerClass(FsmPartitioner.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        // whether to use sequence files as output //TODO: Disabled temporarily
        if (useSequenceFiles) {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
        }

        // double the default block size from 64 MB to 128 MB
        // job.getConfiguration().setLong("dfs.block.size", 134217728);

        job.setMapperClass(FsmMapper.class);

        if (commonConfig.isUseAggregation())
            job.setCombinerClass(FsmCombiner.class);
        job.setReducerClass(FsmReducer.class);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(numberOfReducers);
        job.getConfiguration().set("mapreduce.cluster.reducememory.mb", "4096");

        job.setOutputKeyClass(IntArrayWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

        while (!job.isComplete()) {
            // wait
        }

        return;

    }
}
