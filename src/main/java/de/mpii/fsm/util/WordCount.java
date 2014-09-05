package de.mpii.fsm.util;

import org.apache.mahout.math.map.OpenObjectIntHashMap;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Classic word count -- required by some of the other approaches.
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class WordCount {

    public static final class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outKey = new Text();

        private final IntWritable outValue = new IntWritable(1);

        @Override
        public void map(LongWritable k1, Text v1, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
        	OpenObjectIntHashMap<String> counts = new OpenObjectIntHashMap<String>();
            for (String sentence : v1.toString().split("\n")) {
                String[] terms = sentence.split("\\s+");
                for (String term : terms) {
                    counts.adjustOrPutValue(term, +1, +1);
                }
            }
            for (String term : counts.keys()) {
                outKey.set(term);
                outValue.set(counts.get(term));
                oc.collect(outKey, outValue);
            }
        }

    }

    public static final class Combine extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        private Text outKey = new Text();

        private IntWritable outValue = new IntWritable();

        @Override
        public void reduce(Text k2, Iterator<IntWritable> itrtr, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
            int cnt = 0;
            while (itrtr.hasNext()) {
                cnt += itrtr.next().get();
            }
            outKey.set(k2);
            outValue.set(cnt);
            oc.collect(outKey, outValue);
        }

    }

    public static final class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        private int minSuppotThreshold;

        private Text _word = new Text();

        private IntWritable _count = new IntWritable();

        @Override
        public void configure(JobConf job) {
            minSuppotThreshold = job.getInt("de.mpii.analytics.ngrams.minsup", 1);
        }

        @Override
        public void reduce(Text k2, Iterator<IntWritable> itrtr, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
            int cnt = 0;
            while (itrtr.hasNext()) {
                cnt += itrtr.next().get();
            }
            if (cnt >= minSuppotThreshold) {
                _word.set(k2);
                _count.set(cnt);
                oc.collect(_word, _count);
            }
        }

    }
}
