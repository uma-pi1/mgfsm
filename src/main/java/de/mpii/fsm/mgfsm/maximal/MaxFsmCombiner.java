package de.mpii.fsm.mgfsm.maximal;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import de.mpii.fsm.driver.FsmConfig.Type;


/**
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 *
 */
public class MaxFsmCombiner extends Reducer<BytesWritable, LongWritable, BytesWritable, LongWritable> {
  LongWritable newValue = new LongWritable();

  long v, maxV;
  Type OUTPUT_TYPE;

  public void setup(Context context) throws IOException {
    OUTPUT_TYPE = context.getConfiguration().getEnum("org.apache.mahout.fsm.partitioning.outputType", Type.ALL);
  }
  
  @SuppressWarnings("incomplete-switch")
  @Override
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
    newValue.set(maxV);
    context.write(key, newValue);
  }
}