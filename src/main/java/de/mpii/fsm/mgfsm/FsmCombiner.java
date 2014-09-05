package de.mpii.fsm.mgfsm;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The combiner sums up the weights of identical sequences. It receives (sequence, weight) pairs
 * and outputs pair (sequence, weightSum) where weightSum is the sum of weights
 * 
 * @param key encoded sequence
 * @param value weight of the sequence
 * 
 * @author Iris Miliaraki
 */
class FsmCombiner extends Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable> {

    // stores the total weight of the current transaction
    private final IntWritable supportWrapper = new IntWritable();

    @Override
    protected void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int support = 0;
        for (IntWritable value : values) {
            support += value.get();
        }
        supportWrapper.set(support);
        context.write(key, supportWrapper);
    }
}