/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package de.mpii.fsm.driver;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Parameters;

import de.mpii.fsm.util.Constants;
import de.mpii.fsm.util.Dictionary;


/**
 *-------------------------------------------------------------------------------------
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 *------------------------------------------------------------------------------------- 
 * Utilizes the org.apache.mahout.commmon.Parameters and
 * org.apache.mahout.common.AbstractJob for running the 
 * MG-FSM algorithm according to the user specified parameters.
 * 
 * Argument List :
 * 
 *   1. --support (-s)     (Optional) The minimum number of times the 
 *  	 				             sequence to be mined must be present in the Database
 *             	          (minimum support)
 *        		             Default Value: 1
 *         
 *   2. --gamma   (-g) 	  (Optional) The maximum amount of gap that can be 
 *                    		 taken for a sequence to be mined by MG-FSM,
 *                    		 Default Value: 2
 *                       
 *   3. --lambda  (-l) 		(Optional) The maximum length of the sequence 
 *  				   		         to be mined is determined by the this parameter.
 *                    		 Default Value: 5 
 *                     
 *   4. --execMode(-m)     (Optional) Method of execution 
 *                         viz. (s)equential or (d)istributed 
 *                         Default Value: (s)equential    
 *                         
 *   5. --type      (-t)   (Optional) Specify the output type.
 *                         Expected values for type:
 *                         1. (a)ll 2. (m)aximal 3. (c)losed
 *                         Default Value : (a)ll
 *          
 *   6. --keepFiles (-k)   (Optional) Keep the intermediary files for later 
 *  						           use or runs. The files stored are: 
 *  						           1. Dictionary 2. Encoded Sequences
 *     		                      
 *   7. --resume    (-r)   (Optional) Resume running further runs of 
 *  						           the MG-FSM algorithm on already encoded transaction
 *  						           file located in the folder specified in input.                    
 *                      
 *   8. --input     (-i)   (Optional) Path where the input transactions / database
 *  						           text file is located.
 *  
 *   9. --output    (-o)    Path where the output files are to written.
 *  
 *  10. --tempDir   (-tempDir) (Optional) Specify the temporary directory to be 
 *                              used for the map--reduce jobs.
 *                              
 * 	11. --numReducers (-N)  (Optional) Number of reducers to be used by MG-FSM.
 * 							Default value : 90                              
 *  
 *-------------------------------------------------------------------------------------  
 *  References :
 *-------------------------------------------------------------------------------------
 *  [1] Miliaraki, I., Berberich, K., Gemulla, R., & Zoupanos, 
 *      S. (2013). Mind the Gap: Large-Scale Frequent Sequence Mining.
 *-------------------------------------------------------------------------------------
 *  Notes :
 *-------------------------------------------------------------------------------------   
 *  1. -r and -k are mutually exclusive.    
 *  2. -i and -r are mutually exclusive.
 *  3. Only -(o)utput is the compulsory option. All other are options are optional.  
 *-------------------------------------------------------------------------------------
 */
public final class FsmDriver extends AbstractJob {

  //  private static final Logger log = LoggerFactory.getLogger(FsmDriver.class);

  //Use this configuration object
  //to communicate between every 
  //class
  public FsmConfig commonConfig;


  /* Empty Constructor */
  public FsmDriver() {
    commonConfig = new FsmConfig();
  }

  /**
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   * 
   * Add the appropriate options here. Execute the MG-FSM algorithm 
   * according to the parameters specified at run time.
   * 
   * @param String[] args 
   * @return int
   */
  @Override
  public int run(String[] args) throws Exception {      
    /* Here parameters that will be available to the user 
     * during run time are specified and intialized. */

    /* Hadooop-config options */
    addOutputOption();
    
    /*User-interesting options*/
    addOption("input",
              "i",
              "(Optional) Specify the path from where the input is to be read"
              + "\n NOTE: This option can not be used with -(r)esume option.",
              null);
    
    addOption("support", 
              "s", 
              "(Optional) Minimum support (sigma) " 
              + "\nDefault Value: 1\n", 
              FsmConfig.SIGMA_DEFAULT_STRING);

    addOption("gamma", 
              "g", 
              "(Optional) Maximum allowed for mining frequent sequences (gamma)" +
              " by MG-FSM " + "\nDefault Value: 2\n", 
              FsmConfig.GAMMA_DEFAULT_STRING);

    addOption("lambda", 
              "l",
              "(Optional) Maximum length for mining frequent sequences (lambda)"  +
              "\nDefault Value: 5\n", 
              FsmConfig.LAMBDA_DEFAULT_STRING);

    addOption("execMode",
              "m",
              "Method of execution viz. s -(s)equential or d -(d)istributed" +
              "\nDefault Value: (s)-sequential\n", 
              FsmConfig.DEFAULT_EXEC_MODE);

    addOption("type",
              "t", 
              "(Optional) Specify the mining mode."+
              "\nExpected values for input:" +
              "\n1. a -(a)ll\n2. m -(m)aximal \n3. c -(c)losed" +
              "\nDefault Value : a -(a)ll\n",
              FsmConfig.DEFAULT_TYPE);

    /* keepFiles default value is null.
     * It will be set to a temporary location, in case
     * no path is specified.*/
    addOption("keepFiles", "k", "(Optional) Keep the intermediary files "
        + "for later use or runs. The files stored are:"
        + "\n1. Dictionary \n2. Encoded Sequences \n "
        + "Specify the intermediate path where to keep these files :",
          null);

    /* resume points to the location where the 
     * intermediary files are located*/
    addOption("resume", "r", "(Optional) Resume running further "
        + "runs of the MG-FSM algorithm on"
        + " already encoded transaction file located in the folder specified in input.\n",
          null);

    /*Developer-interesting options*/
    addOption("partitionSize", "p",
              "(Optional) Explicitly specify the partition size." 
            + "\nDefault Value: 10000", 
              FsmConfig.DEFAULT_PARTITION_SIZE);
    
    addOption("indexing", "id", "(Optional) Specify the indexing mode."
            + "\nExpected values for input:"
            + "\n1. none\n2. minmax \n3. full" 
            + "\nDefault Value : full\n", 
              FsmConfig.DEFAULT_INDEXING_METHOD);

    /* split flag is false by default*/
    addFlag("split", "sp", "(Optional) Explicitly specify "
          + "whether or not to allow split by setting this flag.");

    
    addOption("numReducers", "N", "(Optional) Number of reducers to be used by MG-FSM. Default value: 90 ", "90");

    /*------------------------------------------------------------
     * ERROR CHECKS
     *------------------------------------------------------------*/

    /* Parse the arguments received from 
     * the user during run-time.*/
    if (parseArguments(args) == null) {
      System.out.println("\n------------\n"+
                         " E R R O R " +
                         "\n------------\n");
      System.out.println("One of the mandatory options is NOT specified");
      System.out.println("e.g. the input option MUST be specified.");
      //Return a non-zero exit status to indicate failure
      return 1;
    }

    Parameters params = new Parameters();
    if(hasOption("tempDir")){
      String tempDirPath = getOption("tempDir");
      params.set("tempDir", tempDirPath);
    }
    if(hasOption("input")){
      String inputString = getOption("input");
      params.set("input", inputString);
    }
    else{
      params.set("input", null);
    }
    if (hasOption("support")) {
      String supportString = getOption("support");
      /* 
       * Checks & constraints on the value that can
       * be assigned to support, gamma, & lambda.
       * 
       * NOTE: refer [1]
       */
      if(Integer.parseInt(supportString) < 1) {
        System.out.println("Value of support should be greater than or equal to 1");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("support", supportString);

    }
    if (hasOption("gamma")) {
      String gammaString = getOption("gamma");

      if(Integer.parseInt(gammaString) < 0)
      {
        System.out.println("Value of gap should be greater than or equal to 0");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("gamma", gammaString);
    }
    if (hasOption("lambda")) {
      String lambdaString = getOption("lambda");

      if(Integer.parseInt(lambdaString) < 2) {
        System.out.println("Value of length should be greater than or equal to 2");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("lambda", lambdaString);
    }  
    if(hasOption("execMode")){
      String modeString = getOption("execMode");
      params.set("execMode", modeString);
    }
    if(hasOption("type")){
      String modeString = getOption("type");
      params.set("type", modeString);
    }
    if(hasOption("indexing")){
      String indexingString = getOption("indexing");
      params.set("indexing", indexingString);
    }
    if(hasOption("partitionSize")) {
      String partitionString = getOption("partitionSize");
      params.set("partitionSize", partitionString);
    }
    if(hasOption("split")) {
      params.set("split", "true");
    }
    else {
      params.set("split", "false");
    }
    if (hasOption("keepFiles")) {
      String keepFilesString = getOption("keepFiles");
      params.set("keepFiles", keepFilesString);
    } 
    else {
      params.set("keepFiles", null);
    }
    if (hasOption("resume")) {
      String resumeString = getOption("resume");
      params.set("resume", resumeString);
    }
    else {
      params.set("resume", null);
    }
    
    if(hasOption("numReducers")){
    	String numReducersString = getOption("numReducers");
    	params.set("numReducers", numReducersString);
    } else {
    	params.set("numReducers", null);
    }
    
    
    Path inputDir  = null;
    Path outputDir = getOutputPath();
    
    /* ---------------------------------------------------------------------
     * ERROR CHECKS ON COMBINATION OF OPTIONS SUPPLIED TO THE DRIVER
     * --------------------------------------------------------------------*/
    
     //Complain if the '-(t)ype' is equal to '-(m)aximal' or '-(c)losed' and 
     //the 'tempDir' is not specified
     /*if((params.get("tempDir")==null||params.get("tempDir").contentEquals("temp"))&&
        ((params.get("type").toCharArray()[0]=='m')||(params.get("type").toCharArray()[0]=='c'))){
       System.out
          .println("If -(t)ype is -(m)aximal or -(c)losed then a -tempDir path must be specified");
     }*/
     if((params.get("resume")!=null)&&(params.get("keepFiles")!=null)){
       System.out.println("-(r)esume & -(k)eepFiles are mutually exclusive options");
       System.out.println("Exiting...");
       //Return a non-zero exit status to indicate failure
       return (1);
     }
     if((params.get("input")!=null)&&(params.get("resume")!=null)){
       System.out.println("-(r)esume & -(i)nput are mutually exclusive options");
       System.out.println("Exiting...");
       //Return a non-zero exit status to indicate failure
       return (1);
     }
     if((params.get("input")==null)&&(params.get("resume")==null)){
       System.out.println("At least one option from -(i)nput or -(r)esume must be specified");
       System.out.println("Exiting...");
       //Return a non-zero exit status to indicate failure
       return (1);
     }
     else{
           if(params.get("input")!=null){
             inputDir = new Path(params.get("input"));
           }
           else{
             inputDir = new Path(params.get("resume")); 
           }
     }
    /* ---------------------------------------------------------------------
     * Checks to make sure the i/o paths
     * exist and are consistent.
     * --------------------------------------------------------------------
     */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    //If the output paths exist clean them up
    if(fs.exists(outputDir)){
      System.out.println("Deleting existing output path");
      fs.delete(outputDir, true);
    }
    //Create the necessary output paths afresh now
    fs.mkdirs(outputDir);
    
    //Complain if the input path doesn't exist
    if(!fs.exists(inputDir)){
      System.out.println("\n------------\n"+
          " E R R O R " +
          "\n------------\n");

      System.out.println("Input path does not exist OR input option not specified. Exiting...");
      //Return a non-zero exit status to indicate failure
      return (1);
    }
    
    if(inputDir.toString().compareTo(outputDir.toString()) == 0){
      System.out.println("\n------------\n"+
                         " E R R O R " +
                         "\n------------\n");

      System.out.println("The input and output path can NOT be same."
          + "\nThe output path is deleted prior to running the Hadoop jobs."
          + "\nHence, the input would be also deleted if paths are same."
          + "\nExiting...");
      //Return a non-zero exit status to indicate failure
      return (1);
    } 
    
    params.set("input",  inputDir.toString());
    params.set("output", outputDir.toString());

    /*---------------------------------------------------------------------
     * END OF ERROR CHECKS
     * --------------------------------------------------------------------*/
    
    /* Execute the FSM Job depending upon the parameters specified. */ 
    String executionMethod    = getOption("execMode");
    
    //Set the resume and keepFiles flags in the commonConfig.
    //Also, set the intermediateOutput path accordingly.
    if(params.get("resume")!=null)
      commonConfig.setResumeOption(true);
    else
      commonConfig.setResumeOption(false);
    
    if(params.get("keepFiles")!=null){
      commonConfig.setKeepFilesOption(true);
      Path intermediateDir = new Path(params.get("keepFiles"));
      if(fs.exists(intermediateDir)){
    	  fs.delete(intermediateDir, true);
      }
      commonConfig.setIntermediatePath(params.get("keepFiles"));
    }
    else{
      File intermediateOutputPath = File.createTempFile("MG_FSM_INTRM_OP_", "");
      
      //Below JDK 7 we are only allowed to create temporary files.
      //Hence, turn the file into a directory in temporary folder.
      intermediateOutputPath.delete();
      intermediateOutputPath.mkdir();
      
      commonConfig.setIntermediatePath(intermediateOutputPath.getAbsolutePath().toString());
      
      System.out.println("The intermediate output will be written \n"
                          + "to this temporary path :"
                          + intermediateOutputPath);
      
      commonConfig.setKeepFilesOption(false);
    }
    
    //Set the 'tempDir' if its null
    if(params.get("tempDir")==null || params.get("tempDir").contentEquals("temp")){
     
      File tempOutputPath = File.createTempFile("MG_FSM_TEMP_OP_", "");
      
      
      
      tempOutputPath.delete();
      //tempOutputPath.mkdir();
      
      
      commonConfig.setTmpPath(tempOutputPath.getAbsolutePath().toString());
      
      System.out.println("The temporary output associated with the internal map -reduce\n"
                          + "jobs will be written to this temporary path :"
                          + commonConfig.getTmpPath());
    }
    else{
          commonConfig.setTmpPath(params.get("tempDir"));
    }
    
    //Set the input and output paths of the commonConfig
    commonConfig.setInputPath(params.get("input"));
    commonConfig.setOutputPath(params.get("output"));
    commonConfig.setDictionaryPath(commonConfig
                                  .getIntermediatePath()
                                  .concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH));
    
    //Supply the rest of the algorithm specific options to commonConfig
    commonConfig.setSigma(Integer.parseInt(params.get("support")));
    commonConfig.setGamma(Integer.parseInt(params.get("gamma")));
    commonConfig.setLambda(Integer.parseInt(params.get("lambda")));

    commonConfig.setPartitionSize(Long.parseLong(params.get("partitionSize")));
    commonConfig.setAllowSplits(Boolean.parseBoolean(params.get("splits")));
    
    if(params.get("numReducers") != null){
    	commonConfig.setNumberOfReducers(Integer.parseInt(params.get("numReducers")));
    }
    

    switch(params.get("type").toCharArray()[0]){
    case 'a': { commonConfig.setType(FsmConfig.Type.ALL);break; }
    case 'm': { commonConfig.setType(FsmConfig.Type.MAXIMAL); break;}
    case 'c': { commonConfig.setType(FsmConfig.Type.CLOSED); break;}
    default : { commonConfig.setType(FsmConfig.Type.ALL); break;}
    }

    switch(params.get("indexing").toCharArray()[0]){
    case 'n': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.NONE); break; }
    case 'm': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.MINMAX); break;}
    case 'f': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.FULL); break;}
    default : { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.FULL); break;}
    }
    
    
    //SEQUENTIAL EXECUTION MODE
    
    
    if ("s".equalsIgnoreCase(executionMethod)) {    	
      SequentialMode mySequentialMiner;
      
      mySequentialMiner = new SequentialMode(commonConfig);
      
       // If we are dealing with a fresh set of transactions 
       // we need to do encode & then mine.
      
      if(!commonConfig.isResumeOption()) {
        mySequentialMiner.createDictionary(commonConfig.getInputPath());
        mySequentialMiner.createIdToItemMap();
        //If the input path is a corpus 
        //runSeqJob will recursively call encodeAndMine()
        //on all the files to bring together a encoded sequences file
        //and consequently call the sequences miner on each of these
        //encoded sequences
        mySequentialMiner.runSeqJob(new File(commonConfig.getInputPath()));
      }
      /* 
       * If the transactions are encoded from previous runs, then run
       * the following set of functions for reading the encoded transactions
       * and then directly mine them for frequent sequences.  
       */
      else { 	 
        mySequentialMiner.setIdToItemMap(new Dictionary()
                                        .readDictionary(commonConfig
                                                       .getInputPath()
                                                       .concat(
                                                               "/"+Constants
                                                                  .OUTPUT_DICTIONARY_FILE_PATH)));
        
        mySequentialMiner.encodeAndMine(mySequentialMiner.getCommonConfig().getInputPath());
      }
    } 	
    
    
    //DISTRIBUTED EXECUTION MODE
    else if ("d".equalsIgnoreCase(executionMethod)) {
      
      DistributedMode myDistributedMiner = new DistributedMode(commonConfig);
      /*Execute the appropriate job based on whether we need to 
       * encode the input sequences or not.
       */
       if(!commonConfig.isResumeOption())
         myDistributedMiner.runJobs();
       else
         myDistributedMiner.resumeJobs();
       
    }
    //END OF EXECUTING FSM JOB
    //Return a zero exit status to indicate successful completion
    return 0;
  }
  
  /**
   * The main method receives the cmd arguments
   * and initiates the Hadoop job.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception { 
    ToolRunner.run(new Configuration(), new FsmDriver(), args);
  }

}