package de.mpii.fsm.mgfsm.maximal;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.util.IntArrayWritable;


/**
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 *
 */
public class MaxFsmReducer extends Reducer<BytesWritable, LongWritable, IntArrayWritable, LongWritable> {
  IntArrayWritable newKey = new IntArrayWritable();
  LongWritable support = new LongWritable();

  long v, maxV;
  Type OUTPUT_TYPE;
  
  public void setup(Context context) throws IOException {
    OUTPUT_TYPE = context.getConfiguration().getEnum("org.apache.mahout.fsm.partitioning.outputType", Type.ALL);
  }
  
  @SuppressWarnings("incomplete-switch")
  public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    maxV = 0;
    
    switch (OUTPUT_TYPE) {
    case MAXIMAL:
      for (LongWritable value : values) {
        v = value.get();
        maxV = MaxUtils.maxLength(maxV, v);
      }
      break;
      
    case CLOSED:
      for (LongWritable value : values) {
        v = value.get();
        maxV = MaxUtils.maxSupport(maxV, v);
      }
      break;
    }
    int[] sequence = ByteSequence.decode(key);

    if (sequence.length == MaxUtils.rightOf(maxV)) {
      support.set(MaxUtils.leftOf(maxV));
      newKey.setContents(sequence);
      context.write(newKey, support);
    }
  }
}
