package de.mpii.fsm.driver;

import org.apache.hadoop.fs.Path;

/*---------------------------------------------
 * CHECKLIST
 * --------------------------------------------
/*---------------------------------------------
 *Still to do : 
 *---------------------------------------------

// TODO: methods to parse from arguments, method to write to JobConf, methods to read from JobConf
// TODO: driver should expose an option to store sequences in encoded form


/*---------------------------------------------
 * The TODO which are done 
 * ---------------------------------------------
// TODO: no warnings (in your classes)
// TODO: FsmConfig should be used everywhere
// TODO: FSM tester should use the driver, the FsmJob does not need a main method
// TODO: this should not be static
// TODO: everything non-static
// TODO: separate: DriverConfig, FsmConfig --> keep it one (dhgupta)
// TODO: use enums everywhere; e.g., output type ALL / MAXIMAL / CLOSED
// TODO: parsing of CLI arguments; complain when arguments unknown or wrong value etc.
// TODO: DistributedMode and SequentialMode should use common FsmConfig class / parsing etc.
//       and be renamed: SequentialFsmRunner / DistibutedFsmRunner (also in mpii.fsm package)
// TODO: config should have print method / or Mahout code
// TODO: max 100 characters + consistent code formatting
*/

/**
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * FsmConfig class incorporates all the parameters that 
 * are required for running the MG-FSM algorithm.
 * This class is used for communicating the various 
 * values of parameters across different jobs.
 * 
 * For a brief description of each of the parameters 
 * org.apache.mahout.fsm.driver.FsmDriver.java can be consulted, where the values 
 * of these parameters are initialized.
 *
 */

public class FsmConfig {

  //Enumerated Data Types for the 
  //various parameters of the 
  //configuration that optional values.

  /*
   * Types of indexing modes are : 
   * 0. NONE
   * 1. MINMAX
   * 2. FULL
   * 
   * Default - NONE
   */
  public static enum IndexingMethod {
    NONE, MINMAX, FULL
  };

  /*
   * Types of mining modes are : 
   * 0 - all
   * 1 - maximal
   * 2 - closed
   * 
   * Default - all
   */
  public static enum Type{
    ALL, MAXIMAL, CLOSED
  };

  /* Mode
   * 0-Basic
   * 1-Compressed
   * 2-Reduced
   * 3-Aggregated
   * 4-Separated
   * 
   * Default is 3
   */
  public static enum Mode{
    BASIC, COMPRESSED, REDUCED, AGGREAGTED, SEPARATED
  }

  /*
   * Default values of various parameters as "String"
   * literals that are passed to the parameter default options
   * when setting up the Mahout parameters.
   * 
   *  Make the necessary changes here for the driver to allocate 
   *  the specified defaults.
   */
  public static final String GAMMA_DEFAULT_STRING  = "0";
  public static final String SIGMA_DEFAULT_STRING  = "1";
  public static final String LAMBDA_DEFAULT_STRING = "5"; 

  /*
   * Ways to execute the algorithm : 
   * 1. (s) - sequential
   * 2. (d) - distributed
   * 
   * Default - (s) sequential
   */
  public static final String DEFAULT_EXEC_MODE = "s";

  /*
   * Types of mining modes are : 
   * 1. (a) - all
   * 2. (m) - maximal
   * 3. (c) - closed
   * 
   * Default - (a) all
   */
  public static final String DEFAULT_TYPE = "a";

  //Default value for the partition size
  public static final String DEFAULT_PARTITION_SIZE = "10000";

  /*
   * Types of indexing modes are : 
   * 1. none
   * 2. minmax
   * 3. full
   * 
   * Default - (a) all
   */
  public static final String DEFAULT_INDEXING_METHOD = "none";

  public static final int SIGMA_DEFAULT_INT   = 1; 
  public static final int GAMMA_DEFAULT_INT   = 0;
  public static final int LAMBDA_DEFAULT_INT  = 5;
  public static final int DEFAULT_DEBUG_LEVEL = 0;

  //parameters for the MG-FSM algorithm
  private int      sigma; 
  private int      gamma;
  private int      lambda;  

  // i/o paths & bookkeeping parameters
  private String   inputPath; 
  private String   outputPath;
  private String   intermediatePath;
  private boolean  resumeOption;
  private boolean  keepFilesOption;


  //other user-interesting parameters

  // minimum partition size (summing up item frequencies), 
  // items with a frequency > PARTITION_MIN_SIZE are 
  // assigned to a single partition
  private long           partitionSize;

  // Whether encoded transactions can be 'split' 
  private boolean        allowSplits;

  // Whether to build an index for transaction 
  // encoding -- used in case of long transactions
  private IndexingMethod indexingMethod;

  // Mining mode - none,maximal, or closed
  private Type           type;


  // The following attribute sets the debug level
  // for the Map-Reduce jobs.
  private int debugLevel;

  //tmpPath to be used for intermediate output 
  //when the two map--reduce jobs are run in case
  //the -(t)ype is -(m)aximal or -(c)losed
  private String   tmpPath;
  
  //Legacy fields
  private String   typeString;
  private int      mode;
  private String   dictionaryPath;
  private int      debugPartitionId;
  
  private char     outputSequenceChar; // bad name
 
  private boolean  useAggregation;
  private boolean  removeUnreachable;
  private boolean  compressGaps;
  private boolean  bufferTransactions;
  private boolean  useSequenceFiles;
  private int      numberOfReducers;

  private boolean computek1SeqsFrequencies;


  //CONSTRUCTORS

  //Default options values are set here
  public FsmConfig() {

    //basic parameters of the algorithm
    this.sigma  = FsmConfig.SIGMA_DEFAULT_INT;
    this.gamma  = FsmConfig.GAMMA_DEFAULT_INT;
    this.lambda = FsmConfig.LAMBDA_DEFAULT_INT;

    // input output parameters,
    // will be set when the user enters the 
    // values during runtime
    this.inputPath           = null;
    this.outputPath          = null;
    this.intermediatePath    = null;
    this.dictionaryPath      = null;
    this.type                = Type.ALL;

    //other book-keeping parameters
    this.resumeOption     = false;
    this.keepFilesOption  = true;
    this.allowSplits      = false;
    this.debugLevel       = DEFAULT_DEBUG_LEVEL;
    this.indexingMethod   = IndexingMethod.FULL;
    this.partitionSize    = Integer.parseInt(DEFAULT_PARTITION_SIZE);

    //Other developer-interesting options
    //All values are legacy values 
    //carried over from the
    //Config file of "BfsJob"
    this.mode                     = Mode.AGGREAGTED.ordinal();
    this.debugPartitionId         = 0;
    this.numberOfReducers         = 90; 
    this.useAggregation           = true;
    this.removeUnreachable        = true;
    this.compressGaps             = true;
    this.computek1SeqsFrequencies = false;
    this.useSequenceFiles         = true;
    this.tmpPath                  = null; //To be set during the intialization in FsmDriver.java
  }
  
  //Copy Constructor
  public FsmConfig(FsmConfig commonConfig){
    //basic parameters of the algorithm
    this.sigma  = commonConfig.getSigma();
    this.gamma  = commonConfig.getGamma();
    this.lambda = commonConfig.getLambda();

    // input output parameters,
    // will be set when the user enters the 
    // values during runtime
    this.inputPath            = commonConfig.getInputPath();
    this.outputPath           = commonConfig.getOutputPath();
    this.intermediatePath     = commonConfig.getIntermediatePath();
    this.dictionaryPath       = commonConfig.getDictionaryPath();
    this.type                 = commonConfig.getType();

    //other book-keeping parameters
    this.resumeOption     = commonConfig.isResumeOption();
    this.keepFilesOption  = commonConfig.isKeepFilesOption();
    this.allowSplits      = commonConfig.isAllowSplits();
    this.debugLevel       = commonConfig.getDebugLevel();
    this.indexingMethod   = commonConfig.getIndexingMethod();
    this.partitionSize    = commonConfig.getPartitionSize();
    this.tmpPath          = commonConfig.getTmpPath();

    //Other developer-interesting options
    //All values are legacy values 
    //carried over from the
    //Config file of "BfsJob"
    this.mode                     = commonConfig.getMode();
    this.debugPartitionId         = commonConfig.getDebugPartitionId();
    this.numberOfReducers         = commonConfig.getNumberOfReducers(); 
    this.useAggregation           = commonConfig.isUseAggregation();
    this.removeUnreachable        = commonConfig.isRemoveUnreachable();
    this.compressGaps             = commonConfig.isCompressGaps();
    this.computek1SeqsFrequencies = commonConfig.isComputek1SeqsFrequencies();
    this.useSequenceFiles         = commonConfig.isUseSequenceFiles();
  }
  //END OF CONSTRUCTORS

  //GETTER & SETTER METHODS

  /**
   * @return int
   */
  public int getSigma() {
    return sigma;
  }

  /**
   * 
   * @param int sigma
   */
  public void setSigma(int sigma) {
    this.sigma = sigma;
  }

  /**
   * 
   * @return int
   */
  public int getGamma() {
    return gamma;
  }
  /**
   * 
   * @param gamma
   */
  public void setGamma(int gamma) {
    this.gamma = gamma;
  }
  
  /**
   * 
   * @return int
   */
  public int getLambda() {
    return lambda;
  }
  
  /**
   * 
   * @param lambda
   */
  public void setLambda(int lambda) {
    this.lambda = lambda;
  }

  /**
   * 
   * @return String
   */
  public String getInputPath() {
    return inputPath;
  }

  /**
   * 
   * @param String inputPath
   */
  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  /**
   * 
   * @return String
   */
  public String getOutputPath() {
    return outputPath;
  }

  /**
   * 
   * @param String outputPath
   */
  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }
  
  /**
   * 
   * @return boolean
   */
  public boolean isResumeOption() {
    return resumeOption;
  }

  /**
   * 
   * @param boolean resumeOption
   */
  public void setResumeOption(boolean resumeOption) {
    this.resumeOption = resumeOption;
  }
  
  /**
   * 
   * @return boolean
   */
  public boolean isKeepFilesOption() {
    return keepFilesOption;
  }

  /**
   * 
   * @param boolean keepFilesOption
   */
  public void setKeepFilesOption(boolean keepFilesOption) {
    this.keepFilesOption = keepFilesOption;
  }

  /**
   * 
   * @return long
   */
  public long getPartitionSize() {
    return partitionSize;
  }

  /**
   * 
   * @param long partitionSize
   */
  public void setPartitionSize(long partitionSize) {
    this.partitionSize = partitionSize;
  }

  /**
   * 
   * @return IndexingMethod
   */
  public IndexingMethod getIndexingMethod() {
    return indexingMethod;
  }

  /**
   * 
   * @param IndexingMethod indexingMethod
   */
  public void setIndexingMethod(IndexingMethod indexingMethod) {
    this.indexingMethod = indexingMethod;
  }

  /**
   * 
   * @return Type
   */
  public Type getType() {
    return type;
  }

  /**
   * 
   * @param Type type
   */
  public void setType(Type type) {
    this.type = type;
  }

  /**
   * 
   * @return int
   */
  public int getDebugLevel() {
    return debugLevel;
  }

  /**
   * 
   * @param int debugLevel
   */
  public void setDebugLevel(int debugLevel) {
    this.debugLevel = debugLevel;
  }
  
  /**
   * 
   * @return String
   */
  public String getTypeString() {
    return typeString;
  }

  /**
   * 
   * @param String typeString
   */
  public void setTypeString(String typeString) {
    this.typeString = typeString;
  }

  /**
   * 
   * @return int
   */
  public int getMode() {
    return mode;
  }

  /**
   * 
   * @param int mode
   */
  public void setMode(int mode) {
    this.mode = mode;
  }

  /**
   * 
   * @return String
   */
  public String getDictionaryPath() {
    return dictionaryPath;
  }

  /**
   * 
   * @param String dictionaryPath
   */
  public void setDictionaryPath(String dictionaryPath) {
    this.dictionaryPath = dictionaryPath;
  }

  /**
   * 
   * @return int
   */
  public int getDebugPartitionId() {
    return debugPartitionId;
  }

  /** 
   * 
   * @param int debugPartitionId
   */
  public void setDebugPartitionId(int debugPartitionId) {
    this.debugPartitionId = debugPartitionId;
  }


  /**
   * 
   * @return char
   */
  public char getOutputSequenceChar() {
    return outputSequenceChar;
  }

  /**
   * 
   * @param char outputSequenceChar
   */
  public void setOutputSequenceChar(char outputSequenceChar) {
    this.outputSequenceChar = outputSequenceChar;
  }

  /**
   * 
   * @return String 
   */
  public String getTmpPath() {
    return tmpPath;
  }

  /**
   * 
   * @param String tmpPath
   */
  public void setTmpPath(String tmpPath) {
    this.tmpPath = tmpPath;
  }

  /**
   * 
   * @return boolean 
   */
  public boolean isAllowSplits() {
    return allowSplits;
  }

  /**
   * 
   * @param boolean allowSplits
   */
  public void setAllowSplits(boolean allowSplits) {
    this.allowSplits = allowSplits;
  }

  /**
   * 
   * @return boolean
   */
  public boolean isUseAggregation() {
    return useAggregation;
  }

  /**
   * 
   * @param boolean useAggregation
   */
  public void setUseAggregation(boolean useAggregation) {
    this.useAggregation = useAggregation;
  }

  /**
   * 
   * @return boolean
   */
  public boolean isRemoveUnreachable() {
    return removeUnreachable;
  }

  public void setRemoveUnreachable(boolean removeUnreachable) {
    this.removeUnreachable = removeUnreachable;
  }
/**
 * 
 * @return boolean
 */
  public boolean isCompressGaps() {
    return compressGaps;
  }
  /**
   * 
   * @param boolean compressGaps
   */
  public void setCompressGaps(boolean compressGaps) {
    this.compressGaps = compressGaps;
  }

  /**
   * 
   * @return boolean
   */
  public boolean isBufferTransactions() {
    return bufferTransactions;
  }

  /**
   * 
   * @param bufferTransactions
   */
  public void setBufferTransactions(boolean bufferTransactions) {
    this.bufferTransactions = bufferTransactions;
  }

  /**
   * 
   * @return boolean
   */
  public boolean isUseSequenceFiles() {
    return useSequenceFiles;
  }

  /**
   * 
   * @param boolean useSequenceFiles
   */
  public void setUseSequenceFiles(boolean useSequenceFiles) {
    this.useSequenceFiles = useSequenceFiles;
  }

  /**
   * 
   * @return int the number of reducers for the Hadoop job.
   */
  public int getNumberOfReducers() {
    return numberOfReducers;
  }
  /**
   * 
   * @param int the number of reducers for the Hadoop job.
   */
  public void setNumberOfReducers(int numberOfReducers) {
    this.numberOfReducers = numberOfReducers;
  }
  /**
   * 
   * @return boolean
   */
  public boolean isComputek1SeqsFrequencies() {
    return computek1SeqsFrequencies;
  }
  /**
   * 
   * @param boolean computek1SeqsFrequencies
   */
  public void setComputek1SeqsFrequencies(boolean computek1SeqsFrequencies) {
    this.computek1SeqsFrequencies = computek1SeqsFrequencies;
  }
  
  /**
   * 
   * @return String the intermediate path containing the 
   *                encoded sequences and the dictionary
   */
  public String getIntermediatePath() {
    return intermediatePath;
  }
  
  /**
   * 
   * @param intermediatePath the file path which contains
   *                         encoded sequences and the dictionary
   */
  public void setIntermediatePath(String intermediatePath) {
    this.intermediatePath = intermediatePath;
  }

  //END OF GETTER & SETTER METHODS

  Path flistPath;

  public Path getFlistPath() {
    return flistPath;
  }

  public void setFlistPath(Path flistPath) {
    this.flistPath = flistPath;
  }
  
  

}
