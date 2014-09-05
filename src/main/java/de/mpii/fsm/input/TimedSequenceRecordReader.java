package de.mpii.fsm.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

/**
 * RecordReader for sequence data in the following format:
 * 
 * seq-id time-stamp-1 item-1 time-stamp-2 item-2 ... time-stamp-n item-n
 * 
 * Items are ordered by time-stamp
 * 
 * @author Iris Miliaraki (miliaraki@mpi-inf.mpg.de)
 * 
 */
public class TimedSequenceRecordReader extends RecordReader<LongWritable, Text> {

  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

  private LineReader in;

  private LongWritable key;

  private Text initialValue = new Text();

  private Text value = new Text();

  private long start = 0;

  private long end = 0;

  private long pos = 0;

  private int maxLineLength;

  private int maximumFrequency = 1;

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) genericSplit;
    final Path file = split.getPath();
    Configuration conf = context.getConfiguration();
    this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    FileSystem fs = file.getFileSystem(conf);
    start = split.getStart();
    end = start + split.getLength();
    boolean skipFirstLine = false;
    FSDataInputStream filein = fs.open(split.getPath());

    if (start != 0) {
      skipFirstLine = true;
      --start;
      filein.seek(start);
    }
    in = new LineReader(filein, conf);
    if (skipFirstLine) {
      start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    if (initialValue == null) {
      initialValue = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(initialValue, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    // transform sequence
    int multiplyFactor = (2 * maximumFrequency) - 1;
    String sequence = convertTimedSequence(initialValue.toString(), multiplyFactor);

    if (sequence != null) {
      value.set(sequence);
    } else {
      return false;
    }

    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  private String convertTimedSequence(String timedSequence, int multiplyFactor) {
    String[] tokens = timedSequence.split("\\s");

    long sid = Long.parseLong(tokens[0]);
    StringBuilder sb = new StringBuilder();
    sb.append(sid + " ");
    
    long currTime = 0;
    long prevTime = 0;
    long timeDelta = 0;
    long timeGap = 0;
    long item = 0;

    int repeats = 0;

    for (int i = 1; i < tokens.length; i++) {

      // multiply each time-stamp by m=2f-1, where f is the maximum allowed frequency
      // f identical repeated time-stamps [t,t,...,t] will be replaced by [m*t, m*(t+1), m*(t+2), ... m*(t+f-1)]
      currTime = Long.parseLong(tokens[i]) * multiplyFactor;

      if (i != 1) {

        item = Long.parseLong(tokens[i + 1]);
        i++;
        timeDelta = currTime - prevTime;

        if (timeDelta < 0) {
          System.err.println("Wrongly formatted input! ");
          return null;
        }

        if (timeDelta == 0) {

          // replace consecutive identical time-stamps
          repeats++;
          sb.append(item + (i != tokens.length - 1 ? " " : ""));

        } else {

          // new increasing time-stamp, reset repeat counter
          timeGap = timeDelta - repeats - 1;
          repeats = 0;
          if (timeGap > 0) {
            sb.append(-timeGap + " " + item + (i != tokens.length - 1 ? " " : ""));
          } else {
            sb.append(item + (i != tokens.length - 1 ? " " : ""));
          }

        }

        prevTime = currTime;

      } else {

        // first item, no time delta appended
        prevTime = currTime;
        item = Long.parseLong(tokens[i + 1]);
        i++;
        sb.append(item + (i != tokens.length - 1 ? " " : ""));
      }

    }
    return sb.toString();
  }
}