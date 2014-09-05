package de.mpii.fsm.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.mpii.fsm.bfs.BfsMiner;
import de.mpii.fsm.mgfsm.FsmWriterSequential;
import de.mpii.fsm.util.Constants;
import de.mpii.fsm.util.Dictionary;


/**
 * ------------------------------------------------------------------------
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * This class essentially mines the sequences from the 
 * input sequences sequentially (NO distributed mode!)
 * 
 * Following steps are performed (brief overview):
 * 
 * Step 1.a. Construct the dictionary from the input sequences
 *           from the given input sequences specified in the 
 *           input options during the run-time. (Essentially construct
 *           a f-list).
 *
 *                           ---OR---
 *                 
 * Step 1.b. Alternatively read the dictionary stored on the 
 * 			 disk if the user has specified to resume a 
 * 			 previous run of the algorithm. This is done
 * 			 by supplying the path to the  folder containing 
 * 			 the dictionary and the	 encoded sequences. 
 * 
 * Step 2.	 Construct a reverse dictionary i.e. Id to Item/Term
 * 			 mapping for translation of encoded sequences to readable 
 * 			 text sequences.
 *   
 * Step 3.a. Read the input sequences one-by-one and perform the
 * 			 encoding by utilizing the dictionary.
 * 
 * 							  ---OR---
 * Step 3.b. No need for encoding if user has already supplied an 
 * 			 encoded file. Read the transactions one-by-one from the input encoded 
 * 			 sequences file.
 * 
 * Step 4.   Pass these id (integer) encoded sequences to the BfsMiner
 * 			 for mining the frequent sequences.
 * 
 * Step 5.   Perform a reverse translation of sequences from integer id 
 * 		     back to String item /term.
 * 
 * Step 6.   Dump the frequent sequences to a text file 
 * 			 specified at run-time.
 * ------------------------------------------------------------------------
 */

public class SequentialMode {

  //ATTRIBUTES
	private static final String DEFAULT_ITEM_SEPARATOR = "\\s+";
  
  //Common configuration object
  public FsmConfig commonConfig;
  /* Object that will be used to perform the 
   * actual mining of the frequent sequences.
   */
  public BfsMiner myBfsMiner;  

  /*
   * Dictionary used for assigning the ids to the 
   * items read in from the input sequences.
   */
  private Dictionary dictionary;

  /*
   * A map that contains within it the 
   * id to item mappings (Reverse of the dictionary lookup)
   */
  private Map<Integer, String> idToItemMap;

  /* 
   * The sequential writer is required for writing out
   * the frequent sequences to a local disk copy specified
   * in the outputFolderName attribute;
   */
  private FsmWriterSequential seqWriter;

 //END OF ATTRIBUTES

 //CONSTRUCTORS
  
  /**
   *  Empty constructor using default values for parameters.
   *  @param void
   */
  public SequentialMode() { 
    this.myBfsMiner  = new BfsMiner(FsmConfig.SIGMA_DEFAULT_INT, 
                                    FsmConfig.GAMMA_DEFAULT_INT, 
                                    FsmConfig.LAMBDA_DEFAULT_INT);

    this.seqWriter    = new FsmWriterSequential();
    this.idToItemMap  = new HashMap<Integer, String>();
    this.commonConfig = new FsmConfig();
  }

  public SequentialMode(FsmConfig commonConfig) {
    
    this.commonConfig  = commonConfig;
    this.dictionary    = new Dictionary();
    this.myBfsMiner    = new BfsMiner(commonConfig.getSigma(),
                                      commonConfig.getGamma(), 
                                      commonConfig.getLambda());
    this.myBfsMiner.setParametersAndClear(commonConfig.getSigma(), 
                                          commonConfig.getGamma(), 
                                          commonConfig.getLambda(), 
                                          commonConfig.getType());
    
    this.idToItemMap   = new HashMap<Integer, String>();
   
    // The sequential writer attribute will be set when the idToItemMap 
    // is constructed. Here just initialize the seqWriter via empty constructor.
    this.seqWriter   = new FsmWriterSequential(commonConfig.getOutputPath());
  }

  //END OF CONSTRUCTORS

  //GETTER & SETTER METHODS
  
  /**
   * @return FsmConfig
   */
  public FsmConfig getCommonConfig() {
    return commonConfig;
  }

  /**
   * @param FsmConfig commonConfig
   */
  public void setCommonConfig(FsmConfig commonConfig) {
    this.commonConfig = commonConfig;
  }
  
  /**
   * @return org.apache.mahout.fsm.util.Dictionary
   * @param void
   */
  public Dictionary getDictionary() {
    return dictionary;
  }

  /**
   * @return void
   * @param org.apache.mahout.fsm.util.Dictioanry dictionary
   */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * @return Map<Integer, String>
   * @param void
   */
  public Map<Integer, String> getIdToItemMap() {
    return idToItemMap;
  }

  /**
   * @return void
   * @param Map<Integer, String>
   */
  public void setIdToItemMap(Map<Integer, String> idToItemMap) {
    /*
     * Also set the idToItemMap for the 
     * SequentialWriter
     */
    this.seqWriter.setIdToItemMap(idToItemMap);

    this.idToItemMap = idToItemMap;
  }

  //END OF GETTER & SETTTERS

  //METHODS

  /**
   * The following function creates a dictionary
   * using the <i> org.apache.mahout.fsm.util.Dictionary </i> class.
   * For more information concerning the format of the 
   * dictionary consult the class documentation. 
   * 
   * @param String inputFileName - contains the path to the input file 
   * @return void
 * @throws IOException 
   */
  public void createDictionary(String inputFileName) throws IOException {
    /* Construct the dictionary from
     * scratch from the sequence database 
     * pointed by <i> inputFileName </i>.
     */
    this.dictionary = new Dictionary(inputFileName);
    this.dictionary.constructDictionary();
     
    this.dictionary.writeDictionary(this.commonConfig.getIntermediatePath()); 
  }

  /**
   * The following function creates a Map<Integer, String> 
   * that contains within it the translation from <i> int Id </i>
   * to the corresponding <i> String term </i> for converting
   * the encoded input sequences back to readable form.
   * 
   * @param void
   * @return void 
   */
  public void createIdToItemMap()
  {
    /* Simple fetch the <key, value> pairs from the 
     * dictionary by iterating over it and store it 
     * in reverse manner viz. <value (id), key (item)>
     * in the idToItem map.
     */
    Iterator<Entry<String, Dictionary.DicItem>> it = this.dictionary
                                                         .getDictionary()
                                                         .entrySet()
                                                         .iterator();

    while (it.hasNext()) 
    {
      Map.Entry<String, Dictionary.DicItem> pairs = (Map.Entry<String, Dictionary.DicItem>)it.next();

      this.idToItemMap.put(pairs.getValue().getId(), pairs.getKey());
    }


    /*
     * Now, initialize the idToItemMap in the seqWriter by assigning this
     * idToItemMap object reference.
     */
    this.seqWriter.setIdToItemMap(this.idToItemMap);
  }

  /**
   * Recursive function to descend into the directory tree and find all the files 
   * that end with ".txt"
   * 
   * @param dir A file object defining the top directory
 * @throws IOException 
   **/
  public void runSeqJob(File dir) throws IOException
  {
    String pattern = ".txt";
    
    if(dir.isFile() && dir.getName().endsWith(pattern)){
      encodeAndMine(dir);
    }
    else{
      File listFile[] = dir.listFiles();
      if (listFile != null) {
        for (int i=0; i<listFile.length; i++) {
          if (listFile[i].isDirectory()) {
            runSeqJob(listFile[i]);
          } else {
            if (listFile[i].getName().endsWith(pattern)) {
              encodeAndMine(listFile[i]);
            }
          }
        }
      }
    }
  }

  /**
   * This method encode transactions , mines the pattern,
   * files the encoded transaction into a local disk copy.
   *  
   * @param file The input file which contains the textual sequences.
 * @throws IOException 
   */
  public void encodeAndMine(File file) throws IOException {
    
    /*  Clear the bfs object for use     */
    myBfsMiner.clear();
                                                                  
    String dicFilePath = this.commonConfig.getIntermediatePath().concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH);
    int sigma = this.commonConfig.getSigma();
    
    myBfsMiner.setFlistMap(this.dictionary.getFListMap(dicFilePath, sigma));

    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      /* Read the input sequences file during these steps.
       * The input sequences are read one by one and 
       * encoded using the dictionary constructed and stored
       * internally.
       */
      FileInputStream fstream = new FileInputStream(file);

      /*  Get the object of DataInputStream          */
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br  = new BufferedReader(new InputStreamReader(in));
      String strLine;

      /*------------------------------------------------------------------
       * Initialization for writing the encoded sequences to text file on
       * disk.
       *------------------------------------------------------------------
       */
      String outputFileName = this.commonConfig.getIntermediatePath();
      outputFileName        = outputFileName.concat("/"+Constants.OUTPUT_ENCODED_SEQ_FILE_PATH + 
    		  										"/" + Constants.ENCODED_LIST_FILE_NAME);
      File outFile          = new File(outputFileName);
      
      //If parent folder "raw" doesn't exist create it now
      if(!fs.exists(new Path(outputFileName)))
        fs.create(new Path(outputFileName));
      
      BufferedWriter outputBr = new BufferedWriter(new FileWriter(outFile, true));

      //End of initialization

      while ((strLine = br.readLine()) != null) 
      {
        String[] splits = strLine.split("\\s+");

        // write the sequence identifier to the file on local disk
        outputBr.write(splits[0] + "\t");

        // initialize array to form new transaction
        int[] transaction = new int[splits.length - 1];
        int index = 0;

        for(int i = 1; i < splits.length; ++i) {

        	String item = splits[i];

          // look up the id in the dictionary to form
          // the encoded transaction.
          if (this.dictionary.getDictionary().containsKey(item)) {
            transaction[index] = this.dictionary
                .getDictionary()
                .get(item)
                .getId();
          }
          index++;
        }
        
        //write to the transaction to the local disk
        outputBr.write(Arrays.toString(transaction) + "\n");

        // adding transactions to bfsMiner
        myBfsMiner.addTransaction(transaction, 0, transaction.length, 1);
      }
      br.close();

      outputBr.close();

      // mine sequence and SequenceWriter will display (sequence, support)
      myBfsMiner.mineAndClear(this.seqWriter);

    } 
    catch (Exception e) {
      e.printStackTrace();
    }
    
  }

  /**
   * The following function is a overloaded version of the <i> encodeAndMine() <\i>.
   * The following function will take the <i> outputFolder <\i> path and 
   * create the necessary transactions from the already constructed encoded sequence 
   * file and mine the transactions so created.
   * 
   * @return void
   * @param String outputFolder
   */
  public void encodeAndMine(String outputFolder) throws IOException, InterruptedException {

	// clear the bfs object for use
	myBfsMiner.clear();
	  
	String encodedFileName = outputFolder.concat("/" + Constants.OUTPUT_ENCODED_SEQ_FILE_PATH
    										   + "/" + Constants.ENCODED_LIST_FILE_NAME);
    
    String dicFilePath = outputFolder.concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH);
    int sigma = this.commonConfig.getSigma();
    
    myBfsMiner.setFlistMap(this.dictionary.getFListMap(dicFilePath, sigma));
    
    try {     
      /* Read the encoded file during the below steps, and 
       * pass them one-by-one to the BfsMiner object.
       */
      FileInputStream fstream = new FileInputStream(encodedFileName);

      // Get the object of DataInputStream
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String strLine;

      while ((strLine = br.readLine()) != null) {

        /* Remove from the encoded transaction following 
         * 1. "["  
         * 2. "]"
         * 3. "<whitespace>"
         * 4. ","
         * to tokenize it.
         */
        strLine = strLine.replaceAll("[\\s+,]", " ")
            .replaceAll("\\[", " ")
            .replaceAll("\\]", " ");

        //StringTokenizer tokenizer = new StringTokenizer(strLine);
        String[] splits = strLine.split(DEFAULT_ITEM_SEPARATOR);

       	//initialize array to form new transaction
       	int[] transaction = new int[splits.length - 1];


        int index = 0;
        for(int i = 1; i < splits.length; ++i) {
          transaction[index++] = Integer.parseInt(splits[i]);
        }
        // adding transactions to bfsMiner
        myBfsMiner.addTransaction(transaction, 0, transaction.length, 1);
      }
      br.close();
      // mine sequence and SequenceWriter will display (sequence, support)
      myBfsMiner.mineAndClear(this.seqWriter);

    } catch (Exception e) {

      /* Can only occur if file is of inappropriate type*/
      System.out.println("\n------------\n"+
          " E R R O R " +
          "\n------------\n");
      System.out.println("\nInappropriate File Type.\nInput should be a TEXT (.TXT) file.\nExiting...\n");
      System.exit(1);
    }

  }
  //END OF METHODS
}//END OF CLASS
