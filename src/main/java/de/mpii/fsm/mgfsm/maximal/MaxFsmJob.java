package de.mpii.fsm.mgfsm.maximal;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import de.mpii.fsm.driver.FsmConfig;
import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.util.IntArrayWritable;


/**  
 * @author kbeedkar(kbeedkar@mpi-inf.mpg.de)
 */


public class MaxFsmJob {

  private static FsmConfig commonConfig = new FsmConfig();

  public static void setCommonConfig(FsmConfig commonConfig) {
    MaxFsmJob.commonConfig = commonConfig;
  }

  // GETTER & SETTER METHODS
  public static FsmConfig getCommonConfig() {
    return commonConfig;
  }

  public static void runMaxFsmJob() throws IOException, InterruptedException,
      ClassNotFoundException {

    Type outputType = commonConfig.getType();
    int numberOfReducers = commonConfig.getNumberOfReducers();

    Job mCJob = new Job();
    mCJob.setJarByClass(MaxFsmJob.class);
    mCJob.setJobName("MG-FSM+");

    mCJob.getConfiguration().setEnum("org.apache.mahout.fsm.partitioning.outputType", outputType);

    MultipleInputs.addInputPath(mCJob, 
                                new Path(commonConfig.getTmpPath()),
                                SequenceFileInputFormat.class, 
                                MaxFsmMapper.class);
    MultipleInputs.addInputPath(mCJob, 
                                commonConfig.getFlistPath(), 
                                SequenceFileInputFormat.class,
                                MaxFsmMapper.class);
    
    FileOutputFormat.setOutputPath(mCJob, new Path(commonConfig.getOutputPath()));

    mCJob.setSortComparatorClass(BytesWritable.Comparator.class);

    mCJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    mCJob.setCombinerClass(MaxFsmCombiner.class);
    mCJob.setReducerClass(MaxFsmReducer.class);

    mCJob.setMapOutputKeyClass(BytesWritable.class);
    mCJob.setMapOutputValueClass(LongWritable.class);

    mCJob.setNumReduceTasks(numberOfReducers);
    mCJob.getConfiguration().set("mapreduce.cluster.reducememory.mb", "4096");

    mCJob.setOutputKeyClass(IntArrayWritable.class);
    mCJob.setOutputValueClass(LongWritable.class);

    mCJob.waitForCompletion(true);

    while (!mCJob.isComplete()) {
    }

  }
}
