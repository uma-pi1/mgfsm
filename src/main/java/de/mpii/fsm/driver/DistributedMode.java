package de.mpii.fsm.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.mgfsm.FsmJob;
import de.mpii.fsm.mgfsm.maximal.MaxFsmJob;
import de.mpii.fsm.output.SequenceTranslator;
import de.mpii.fsm.tools.ConvertSequences;
import de.mpii.fsm.util.Constants;


/**
 * 
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * The following class allows for the running of the MG-FSM algorithm
 * as per the following sequence of steps :
 * 
 * Step 1. a.Compute the f-list of the input data by running a mapreduce wordcount.
 * 		     b.Assign the most frequent items with the smallest integer id.
 * 		       
 *         Above actions performed by org.apache.mahout.fsm.tools.ConvertSequences.java.
 * 
 * Step 2. Mine the encoded sequences using de.mpii.partitioning.BfsJob.java
 * 
 * Step 3. Convert the encoded frequent sequences to translated frequent sequences.
 * 		     This is done by executing the job in org.apache.mahout.fsm.output.SequenceTranslator.
 * 
 * NOTE :  If the input is consists of already encoded transactions then execute resumeJobs(),
 * 	       wherein the Step 2 and Step 3 are carried out only. Otherwise runJobs() should be 
 * 		     executed.
 *
 */
public class DistributedMode
{
  //Attributes
  //Default options
  private static final String DEFAULT_ITEM_SEPARATOR = "\\s+";

  // A common config object containing all the 
  // necessary parameters for executing a 
  // MG-FSM job.
  FsmConfig commonConfig;

  // The following attribute is 
  // used for initializing the item 
  // separator for the SequenceTranslator.java
  // class
  private String itemSeparator;

  //End of attributes

  //Constructors
  /**
   * Empty constructor - Do NOT use
   * this input and output paths in 
   * commonConfig by default are set to null.
   */
  public DistributedMode()
  {
    this.itemSeparator    = DEFAULT_ITEM_SEPARATOR;
    this.commonConfig     = new FsmConfig();
  }
  /**
   * Parameterized constructor
   * @param sigma
   * @param gamma
   * @param lambda
   * @param inputFilePath
   * @param outputFolderPath
   * @param typeString
   * @param indexingMethod
   * @param partitionSize
   * @param allowSplit
   * @param keepFiles
   */
  public DistributedMode(FsmConfig commonConfig) {
    this.itemSeparator    = DEFAULT_ITEM_SEPARATOR;
    this.commonConfig     = commonConfig;
  }
  //END OF CONSTRUCTORS

  //METHODS

  /**
   * The following method will invoke the necessary MapReduce
   * jobs for the MG-FSM algorithm to run in the distributed mode
   * from scratch (i.e. initial input is a text file).
   * 
   * @param void
   * @return void
   */
  public void runJobs()
  {
    String toolRunnerArgs[] 	 = {  this.commonConfig.getInputPath(),
                                    this.commonConfig.getIntermediatePath(),
                                    Integer.toString(this.commonConfig.getNumberOfReducers()),
                                    this.itemSeparator};
    
    /*
     * Assign the BfsJob specific parameters.
     * They should be passed when setting up 
     * the DistributedJob object.
     */
    FsmJob.setCommonConfig(new FsmConfig(this.commonConfig));
    
    //Prepare the input and output options for FsmJob to run
    FsmJob.getCommonConfig().setInputPath(this.commonConfig
                                              .getIntermediatePath()
                                              .concat("/"+Constants
                                              .OUTPUT_ENCODED_SEQ_FILE_PATH));
    FsmJob.getCommonConfig().setOutputPath(this.commonConfig
                                               .getOutputPath()
                                               .concat("/"+Constants
                                               .ENCODED_FREQ_SEQ_FILE_PATH));

    //Prepare the arguments for the seqTranslatorArgs map--reduce job to run.
    String seqTranslatorArgs[] = { 	this.commonConfig
                                        .getOutputPath()
                                        .concat("/" + Constants.ENCODED_FREQ_SEQ_FILE_PATH),
                                    this.commonConfig.getOutputPath()
                                                     .concat("/" + Constants
                                                     .TRANSLATE_FREQ_SEQ_FILE_PATH),
                                    this.commonConfig.getIntermediatePath()
                                                     .concat("/" + Constants
                                                     .OUTPUT_DICTIONARY_FILE_PATH) };

    try {
      /* Delete the output folder if present */
      prepJobs();
      /* Run Step 1, Step 2, & Step 3. */
      /* Step 1. Construct dictionary and encode input sequences. */
      ToolRunner.run(new ConvertSequences(), toolRunnerArgs);
     
      /* Step 2. Mine the frequent sequences. */
      FsmJob.runFsmJob();
      
      // If -t options is provided with m(aximal) or c(losed)
      if(commonConfig.getType() != Type.ALL) {
        MaxFsmJob.setCommonConfig(FsmJob.getCommonConfig());
        MaxFsmJob.runMaxFsmJob();
      }
      
      
      /*
       * Step 3. Perform translation from encoded 
       * sequences to text.
       */
      SequenceTranslator.main(seqTranslatorArgs);

    } catch (Exception e) {
      e.printStackTrace();
    }	  
  }

  /**
   * The following method will invoke the necessary MapReduce
   * jobs to resume running the MG-FSM algorithm on an already 
   * encoded serialized HDFS file.
   * 
   * @param void
   * @return void
   */
  public void resumeJobs()
  {
    //Prepare the arguments for the seqTranslatorArgs map--reduce job to run.
    String seqTranslatorArgs[] = { 	this.commonConfig.getIntermediatePath()
                                                     .concat("/"+ Constants
                                                     .ENCODED_FREQ_SEQ_FILE_PATH),
                                    this.commonConfig.getOutputPath()
                                                     .concat("/"+ Constants
                                                     .TRANSLATE_FREQ_SEQ_FILE_PATH),
                                    this.commonConfig.getInputPath()
                                                     .concat("/"+ Constants
                                                     .OUTPUT_DICTIONARY_FILE_PATH)};
    /*
     * Assign the BfsJob specific parameters.
     * They should be passed when setting up 
     * the DistributedJob object.
     */
    FsmJob.setCommonConfig(new FsmConfig(this.commonConfig));
    
    //Prepare the input and output options for FsmJob to run
    FsmJob.getCommonConfig().setInputPath(this.commonConfig
                                              .getInputPath()
                                              .concat("/"+Constants
                                              .OUTPUT_ENCODED_SEQ_FILE_PATH));
    FsmJob.getCommonConfig().setOutputPath(this.commonConfig
                                               .getIntermediatePath()
                                               .concat("/"+Constants
                                               .ENCODED_FREQ_SEQ_FILE_PATH));
    FsmJob.getCommonConfig().setDictionaryPath(this.commonConfig
                                                   .getInputPath()
                                                   .concat("/"+ Constants
                                                   .OUTPUT_DICTIONARY_FILE_PATH));
    
    
    
    try {
      /* Delete the output folder if present */
      prepJobs();
      /* Run Step 2, & Step 3.*/

                                                      
      /* Step 2. Mine the frequent sequences. */
      FsmJob.runFsmJob();
      
      // If -t options is provided with m(aximal) or c(losed)
      if(commonConfig.getType() != Type.ALL) {
        MaxFsmJob.setCommonConfig(FsmJob.getCommonConfig());
        MaxFsmJob.runMaxFsmJob();
      }

      /*
       * Step 3. Perform translation from encoded 
       * sequences to text.
       */
      SequenceTranslator.main(seqTranslatorArgs);
      
    } catch (Exception e) {
      e.printStackTrace();
    }	  
  }
  /**
   * Delete the output folder if it exists before 
   * running any jobs.
   * 
   * @param void 
   * @return void
   */
  public void prepJobs() throws IOException
  {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(new Path(this.commonConfig.getOutputPath())))
       fs.delete(new Path(this.commonConfig.getOutputPath()), true);
  }

  /**
   * Delete the encoded frequent sequences files and the 
   * dictionary files if the <i> keepFiles </i> flag is not 
   * set.
   * 
   * @return void
   * @param  void
   */
  public void cleanUp() throws IOException
  {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    
    Path encodedFSPath   = new Path(this.commonConfig.getOutputPath()
                                        .concat("/"+ Constants.ENCODED_FREQ_SEQ_FILE_PATH));
    Path dictionaryPath  = new Path(this.commonConfig.getInputPath()
                                        .concat("/"+ Constants.OUTPUT_DICTIONARY_FILE_PATH));

    fs.delete(encodedFSPath, true);
    fs.delete(dictionaryPath, true);

    /*
     * Delete these files if they exist.
     */
    if(fs.exists(new Path(this.commonConfig.getOutputPath().concat("/"+"raw"))))
      fs.delete(new Path(this.commonConfig.getOutputPath().concat("/"+"raw")),true);

    if(fs.exists(new Path(this.commonConfig.getOutputPath().concat("/"+"wc"))))
      fs.delete(new Path(this.commonConfig.getOutputPath().concat("/"+"wc")),true);

  }

  //END OF METHODS




}
