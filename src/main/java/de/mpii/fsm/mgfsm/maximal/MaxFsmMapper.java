package de.mpii.fsm.mgfsm.maximal;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import de.mpii.fsm.util.IntArrayWritable;


/**
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 *
 */
public class MaxFsmMapper extends Mapper<IntArrayWritable, LongWritable, BytesWritable, LongWritable> {
  int pivot;
  int length;
  int support;
  int[] sequence;
  
  ByteSequence subSequence = new ByteSequence();

  BytesWritable newKey = new BytesWritable();
  LongWritable newValue = new LongWritable();

  public void map(IntArrayWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
    sequence = key.getContents();
    length = sequence.length;
    support = (int) value.get();
    
    newValue.set(MaxUtils.combine(support,length));

    pivot = sequence[0];
    for (int offset = 1; offset < length; ++offset) {
      if (sequence[offset] > pivot) {
        pivot = sequence[offset];
      }
    }
    for (int offset = 0; offset < length; ++offset) {
      if (sequence[offset] == pivot) {
        if (!subSequence.isEmpty()) {
          emitSubSequence(context);
        }
        continue;
      }
      emitItem(sequence[offset], context);
      subSequence.add(sequence[offset]);
    }
    if (!subSequence.isEmpty()) {
      emitSubSequence(context);
    }
    emitItem(pivot,context);
    emitSequence(context);
  }

  public void emitItem(int item, Context context) throws IOException, InterruptedException {
    ByteSequence.encode(new int[]{item}, newKey);
    context.write(newKey, newValue);
  }
  
  public void emitSequence(Context context) throws IOException, InterruptedException {
    ByteSequence.encode(sequence, newKey);
    context.write(newKey, newValue);
  }

  public void emitSubSequence(Context context) throws IOException, InterruptedException {
    newKey.set(subSequence.get());
    context.write(newKey, newValue);
    subSequence.clear();
  }
}
