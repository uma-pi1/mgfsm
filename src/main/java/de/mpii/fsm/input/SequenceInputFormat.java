package de.mpii.fsm.input;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * InputFormat for sequence data in the following format:
 * 
 *  seq-id  length  item-1  item-2 item-{length}
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class SequenceInputFormat extends FileInputFormat<LongWritable, Text> {

    class SequenceRecordReader implements RecordReader<LongWritable, Text> {

        private LineRecordReader lineReader;

        private LongWritable lineKey;

        private Text lineValue;

        public SequenceRecordReader(JobConf job, FileSplit split) throws IOException {
            lineReader = new LineRecordReader(job, split);
            lineKey = lineReader.createKey();
            lineValue = lineReader.createValue();
        }

        @Override
        public boolean next(LongWritable k, Text v) throws IOException {
            if (!lineReader.next(lineKey, lineValue)) {
                return false;
            }

            String[] tokens = lineValue.toString().split("\\s");

            long did = Long.parseLong(tokens[0]);

            StringBuilder sb = new StringBuilder();
            for (int i = 2; i < tokens.length; i++) {
                sb.append(tokens[i] + (i != tokens.length - 1 ? " " : ""));
            }

            k.set(did);
            v.set(sb.toString());

            return true;
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return lineReader.getPos();
        }

        @Override
        public void close() throws IOException {
            lineReader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return lineReader.getProgress();
        }

    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        rprtr.setStatus(is.toString());
        return new SequenceRecordReader(jc, (FileSplit) is);
    }

}
