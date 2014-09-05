package de.mpii.fsm.util;

/**
 * This class is used to declare the global constants of our implementation. 
 * 
 * @author Spyros Zoupanos
 */


/**
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * This file contains :
 * 1. The global constants/ conventions that are 
 *    used for naming the files and the paths where they can be found.
 * 
 */

// TODO: remove, merge with config etc.
  public final class Constants 
  {

    // TODO: move to dictionary class
	  public static final int ITEM = 0;
	  public static final int COL_FREQ = 1;
	  public static final int DOC_FREQ = 2;
	  public static final int ITEM_ID = 3;
	  
	  
	  /*---------------------------------------------------------------------
	   * 1. The global constants/ conventions that are 
	   *    used for naming the files and the paths where they can be found.
	   *---------------------------------------------------------------------    
	   */
	  
	  /*
	   * Standard path to which the <i> translatedFS.txt </i> will be written to.
	   * i.e. {file_path_specified_by_user}/TRANSLATED_FREQ_SEQ_FILE_PATH 
	   */
	  public static final String TRANSLATE_FREQ_SEQ_FILE_PATH = "translatedFS";
	
		/*
		 * Standard path to which the <i> dictionary.txt </i> will be written to.
		 * i.e. {file_path_specified_by_user}/OUTPUT_DICTIONARY_FILE_PATH 
		 */
	  public static final String OUTPUT_DICTIONARY_FILE_PATH = "wc/part-r-00000";
	  
	  /*
	   * File name of the dictionary is : dictionary.txt
	   */
	  public static final String DICT_FILE_NAME = "part-r-00000";
	  
	  /*
	   * File name of the translated frequent sequences file is : encodedFS.txt
	   */
	  public static final String TRANSLATED_FREQ_SEQ_FILE_NAME = "translatedFS";
	  
	  /*
	   * Standard path to which the <i> encodedSequences.txt </i> will be written to.
	   * i.e. {file_path_specified_by_user}/OUTPUT_ENCODED_SEQ_FILE_PATH 
	   */
	  public static final String OUTPUT_ENCODED_SEQ_FILE_PATH = "raw";
	  
	  /*
	   * File path for the encoded sequences file when running in the sequential mode.
	   */
	  public static final String ENCODED_SEQ_FILE_PATH_SEQMODE = "raw/part-r-00000";
	  
	  /* 
	   * File name for the encodedSequences is : part-r-00000
	   */
	  public static final String ENCODED_LIST_FILE_NAME = "part-r-00000";
	  
	  /*Standard path to which encoded frequent sequences will be written to*/
	  public static final String ENCODED_FREQ_SEQ_FILE_PATH = "encodedFS";
	  
	  /*File name to which the encoded frequent sequences will be written to*/
	  public static final String ENCODED_FREQ_SEQ_FILE_NAME = "part-r-00000";
	  
	  
	  
	  
}
