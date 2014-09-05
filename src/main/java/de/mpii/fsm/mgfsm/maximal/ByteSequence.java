package de.mpii.fsm.mgfsm.maximal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 *
 */
public class ByteSequence {

	private BytesWritable out = new BytesWritable();
	private OutputStream os;
	private DataOutputStream dos;
	
	ByteSequence() {
		out.setSize(0);
		out.setCapacity(200);
		
		os = new OutputStream() {
			@Override
			  public void write(int b) throws IOException {
			    int length = out.getLength();
			    out.getBytes()[length] = (byte) b;
			    out.setSize(length + 1);
			  }
		};
		dos = new DataOutputStream(os);
	}
	
	public void add(int val) throws IOException {
		WritableUtils.writeVInt(dos, val);
	}
	
	public boolean isEmpty() {
		return out.getLength() == 0;
	}
	
	public void clear() throws IOException {
		out.setSize(0);
	}
	
	public BytesWritable get() {
		return out;
	}
	
	public static void encode(int[] sequence, final BytesWritable out) throws IOException {
		out.setSize(0);
	    out.setCapacity(sequence.length * 10);

	    OutputStream os = new OutputStream() {
	      @Override
	      public void write(int b) throws IOException {
	        int length = out.getLength();
	        out.getBytes()[length] = (byte) b;
	        out.setSize(length + 1);
	      }
	    };
	    DataOutputStream dos = new DataOutputStream(os);
		for (int i : sequence) {
			WritableUtils.writeVInt(dos, i);
		}
	}
	
	public static int[] decode(BytesWritable value) throws IOException {
		int[] buff = new int[50];
		int length = 0;
		int[] sequence;
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()));
		while (in.available() > 0) {
			buff[length] = WritableUtils.readVInt(in);
			length++;
		}
		in.close();
		sequence = new int[length];
		System.arraycopy(buff, 0, sequence, 0, length);
		return sequence;
	}	
	
	//-----------
	//naive tests
	//-----------
	public static void main(String [] args) throws IOException {
		
		test(new int[]{1,12,4646,452345,345343,345});
		test(new int[]{1,12,132123,34});
		test(new int[]{1,12,432,23,234,234,234,12,43,132,43,123});
		test(new int[]{1,12,432,23,234,234,234,12,43,132,43,123,0,0,0,6,756756,567567});
		
		ByteSequence s = new ByteSequence();
		s.add(10);
		s.add(12434);
		s.add(3421342);
		
		if(!s.isEmpty()) {
			BytesWritable out = new BytesWritable();
			out.set(s.get());
			int[]r = ByteSequence.decode(out);
			System.out.println(Arrays.toString(r));
		}
		s.clear();
		s.add(101);
		s.add(124);
		s.add(342);
		
		if(!s.isEmpty()){
			BytesWritable out = new BytesWritable();
			out.set(s.get());
			int[]r = ByteSequence.decode(out);
			System.out.println(Arrays.toString(r));
		}
		
		s.clear();
		if(s.isEmpty()){
			System.out.println("Empty");
		}
				
	}
	public static void test(int[] sequence) throws IOException{
		BytesWritable out = new BytesWritable();
		ByteSequence.encode(sequence, out);
	    
	    int[] res = ByteSequence.decode(out);
	    System.out.println("Seq = "+ Arrays.toString(sequence));
	    System.out.println("dsq = " + Arrays.toString(res));
	}
}
