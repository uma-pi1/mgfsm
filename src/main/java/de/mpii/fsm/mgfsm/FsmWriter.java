package de.mpii.fsm.mgfsm;

import java.io.IOException;

/**
 * A writer interface that receives a sequence and the number of its occurrences and 
 * it outputs them to a predefined output.
 * 
 * @author Spyros Zoupanos
 */
public interface FsmWriter {
	void write(int[] sequence, long count) throws IOException, InterruptedException;
}
