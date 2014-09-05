package de.mpii.fsm.mgfsm;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import de.mpii.fsm.bfs.BfsMiner;
import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.util.IntArrayWritable;


/**
 * Reduce class: collects sequences per partition and for each partition runs a frequent
 * sequence mining algorithm against them. The sequences have been sorted per partition. Reducer
 * reads (id+sequence, count) pairs and emits one (sequence, support) pair for each frequent
 * sequence discovered by the frequent sequence mining algorithm.
 * 
 * @param key partition identifier
 * @param values encoded subsequences of partition with id key
 */
public class FsmReducer extends Reducer<BytesWritable, IntWritable, IntArrayWritable, LongWritable> {

    // an index mapping partitions to their set of items
    Map<Integer, TreeSet<Integer>> partitionToItems;

    // previously processed key
    BytesWritable prevKey = new BytesWritable();

    // decoded sequence of the previously processed key
    int[] prevSequence = new int[10];

    // size of the decoded sequence of the previously processed key
    int prevSequenceSize = 0;

    // partition ID of the previously processed sequence and its support
    int prevPartitionId = -1;

    int prevSupport = 0;

    // instance of the BFS algorithm initialized with dummy values
    BfsMiner bfs = new BfsMiner(1, 0, 2);

    // a writer object for the mining result sequences
    FsmWriterForReducer writer = new FsmWriterForReducer();

    /** Retrieve cache files from distributed cache */
    @SuppressWarnings("unchecked")
    protected void setup(Context context) throws IOException {
        int sigma = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.sigma", -1);
        int gamma = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.gamma", -1);
        int lambda = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.lambda", -1);
        //int outputSequenceType = context.getConfiguration().getInt("org.apache.mahout.fsm.partitioning.outputSequenceType", Constants.ALL);
        Type outputType = context.getConfiguration().getEnum("org.apache.mahout.fsm.partitioning.outputType", Type.ALL);
        
        
        boolean bufferTransactions  = context.getConfiguration().getBoolean("org.apache.mahout.fsm.partitioning.bufferTransactions", false);
        bfs.setBufferTransactions(bufferTransactions);
        bfs.clear();
        // bfs.setParametersAndClear(sigma, gamma, lambda);
        bfs.setParametersAndClear(sigma, gamma, lambda, outputType);
        prevPartitionId = -1;
        prevKey.setSize(0);

        // read map from partitions to item identifiers from distributed
        // cache
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream("partitionToItems"));
            partitionToItems = (HashMap<Integer, TreeSet<Integer>>) is.readObject();
            is.close();
        } catch (IOException e) {
            FsmJob.LOGGER.severe("Reducer: error reading from dCache: " + e);
        } catch (ClassNotFoundException e) {
            FsmJob.LOGGER.severe("Reducer: deserialization exception: " + e);
        }
    }

    /**
     * Key is the partition id followed by sequence items and value is the weight of the
     * sequence
     */
    protected void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // if it is the same sequence as before, simply update the support
        // and return
        if (key.equals(prevKey)) {
            for (IntWritable value : values) {
                prevSupport += value.get();
            }
            return;
        }
        // when this line is reached, we found a new sequence

        // we have the support of the previous key; add it to the BFS input
        if (prevPartitionId != -1) {
            bfs.addTransaction(prevSequence, 0, prevSequenceSize, prevSupport);
        }

        // initialize new sequence
        prevKey.set(key);
        prevSupport = 0;
        for (IntWritable value : values) {
            prevSupport += value.get();
        }

        // decode the new sequence
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(key.getBytes(), 0, key.getLength()));
        int partitionId = WritableUtils.readVInt(in); // first byte is partition id
        prevSequenceSize = 0;
        while (in.available() > 0) {

            // resize prevSequence if too short
            if (prevSequence.length == prevSequenceSize) {
                int[] temp = prevSequence;
                prevSequence = new int[2 * prevSequence.length];
                System.arraycopy(temp, 0, prevSequence, 0, temp.length);
            }

            // read item / gap
            prevSequence[prevSequenceSize] = WritableUtils.readVInt(in);
            prevSequenceSize++;
        }
        in.close();

        // if it is also a new partition, run BFS on old partition and
        // reinitialize
        if (partitionId != prevPartitionId) {
            if (prevPartitionId != -1) {
                writer.setContext(context);
                bfs.mineAndClear(writer);
            }

            // determine the pivot range [beginItem, endItem] of the
            // partition and initialize
            // BFS
            TreeSet<Integer> items = partitionToItems.get(partitionId);
            int beginItem = items.first();
            int endItem = items.last();
            bfs.initialize(beginItem, endItem);
        }
        prevPartitionId = partitionId;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // run BFS on last partition
        if (prevPartitionId != -1) {
            bfs.addTransaction(prevSequence, 0, prevSequenceSize, prevSupport);
            writer.setContext(context);
            bfs.mineAndClear(writer);
        }
    }
}