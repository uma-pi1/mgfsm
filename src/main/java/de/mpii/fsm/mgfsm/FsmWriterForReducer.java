package de.mpii.fsm.mgfsm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import de.mpii.fsm.util.IntArrayWritable;


/**
 * An implementation of the FSMWriter interface. It is initialized with the Context object
 * of the reducer and every time the write method is called, the given sequence with the 
 * number of its occurrences are written to the Context. The sequence is encoded using
 * the variable length long encoding before the write. 
 * 
 * @author Spyros Zoupanos
 */
public class FsmWriterForReducer implements FsmWriter {

	Reducer<?, ?, IntArrayWritable, LongWritable>.Context context;
	IntArrayWritable key = new IntArrayWritable();
	LongWritable value = new LongWritable();
	
	public FsmWriterForReducer()
	{
		//context = givenCont;
		//key = 
		//value = new LongWritable();
	}
	
	public void setContext(Reducer<?, ?, IntArrayWritable, LongWritable>.Context givenCont) {
	  context = givenCont;
	}
	
	@Override
	public void write(int[] sequence, long count) throws IOException, InterruptedException
	{
	  key.setContents(sequence);
	  value.set(count);
	  context.write(key, value);
	}
}
