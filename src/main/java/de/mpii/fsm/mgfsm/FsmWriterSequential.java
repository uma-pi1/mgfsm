package de.mpii.fsm.mgfsm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import de.mpii.fsm.util.Constants;



/**
 * 
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * The following class performs:
 * 
 * 1. Translation of encoded frequent sequence to 
 *    text, by using the idToItemMap.
 * 
 * 2. Writing out the translated frequent sequences
 *    to a local disk copy.
 *
 */

public class FsmWriterSequential extends FsmWriterForReducer 
{
  /*--------------------------
   * ATTRIBUTES
   *--------------------------
   */

  /*
   * A map that contains within it the 
   * id to item mappings (Reverse of the dictionary lookup)
   */
  private Map<Integer, String> idToItemMap;

  /* 
   * output folder name where the 
   * final text file will be saved.
   */
  private String outputFolderName;

  /* for the output integer sequence pattern */
  int[] seq;

  /* count of pattern */
  long support;



  /*--------------------------
   * END OF ATTRIBUTES
   *--------------------------
   */

  /*--------------------------
   * CONSTRUCTORS
   *--------------------------
   */

  public FsmWriterSequential()
  {
    this.idToItemMap 		= null;
    this.seq		   		= null;
    this.support     		= 0;
    this.outputFolderName = null;
  }

  public FsmWriterSequential(String outputFolderName)
  {
    this.support    = 0;
    this.seq				= null;

    /*
     * The idToItemMap will be assigned when the 
     * corresponding idToItemMap is set in the 
     * org.apache.mahout.fsm.sequential.SequentialMode.java
     * class
     */
    this.idToItemMap 		= null;
    this.outputFolderName = outputFolderName.concat("/"+Constants.TRANSLATED_FREQ_SEQ_FILE_NAME);


  }


  /*---------------------------
   * GETTER & SETTER METHODS
   *---------------------------
   */

  /**
   * @return int[]
   * @param void
   */
  public int[] getSequence() {
    return seq;
  }

  /**
   * @return void
   * @param int[] sequence
   */
  public void setSequence(int[] sequence){
    this.seq = sequence;
  }

  /**
   * @return long
   * @param void
   */
  public long getSupport() {
    return support;
  }

  /**
   * @return void
   * @param long support
   */
  public void setSupport(long support){
    this.support = support;
  }

  /**
   * @param  void
   * @return Map<Integer, String> return the object reference to the reverse dictionary.
   */
  public Map<Integer, String> getIdToItemMap() {
    return idToItemMap;
  }

  /**
   * @param void 
   * @return Map<integer, String> idToItemMap sets the reverse dictionary.
   */
  public void setIdToItemMap(Map<Integer, String> idToItemMap) {
    this.idToItemMap = idToItemMap;
  }

  /**
   * @param void
   * @return String outputFolderName returns the path to the output file.
   */
  public String getOutputFolderName() {
    return outputFolderName;
  }

  /**
   * @param outputFolderName sets the path to the output folder.
   * @return void
   */
  public void setOutputFolderName(String outputFolderName) {
    this.outputFolderName = outputFolderName;
  }


  /*--------------------------------
   * END OF GETTER & SETTER METHODS
   * -------------------------------
   */


  /*--------------------------
   * METHODS
   *--------------------------
   */

  /**
   * Function to collect sequence and count form BfsMiner and translate back to the original form 
   * The additionally functionality introduced is writing to external file on the disk 
   * both encoded as well as the translated version.
   * 
   *  @return - void
   *  @param  - int[] sequence
   *  @param  - long count
   *  
   */
  @Override
  public void write(int[] sequence, long count) throws IOException, InterruptedException {

    this.seq = sequence;
    this.support = count;       

    try 
    {		    	    
      BufferedWriter br = new BufferedWriter(new FileWriter(this.outputFolderName, true));		 

      /*
       * Write the (Sequence, support) each time BfsMiner find a new 
       * sequence that is interesting
       */		   		     
      for (int i = 0; i < sequence.length; i++){
        /*Translation back to original format */  	        	
        br.write(this.idToItemMap.get(sequence[i]) + " ");
      }

      br.write("\t" + support + "\n");

      br.close();

    }   
    catch (IOException e) 
    {    		
      e.printStackTrace();
    }

  }

  /*--------------------------
   * END OF METHODS
   *--------------------------
   */
}
