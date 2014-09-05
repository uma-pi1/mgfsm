package de.mpii.fsm.tools;



import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.list.IntArrayList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import de.mpii.fsm.util.DfsUtils;
import de.mpii.fsm.util.IntArrayWritable;
import de.mpii.fsm.util.PostingWritable;


public class ConvertTextSentences  extends Configured implements Tool {

    //////
    /////
    //// PHASE 1: Perform simple word count
    ///
    //
    public static final class WordCountMapper extends Mapper<LongWritable, Text, Text, PostingWritable> {

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        // singleton output value -- for efficiency reasons
        private final PostingWritable outValue = new PostingWritable();

        // maximum number of documents per file
        private int maxdocs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            maxdocs = context.getConfiguration().getInt("org.apache.mahout.fsm.maxdocs", Integer.MAX_VALUE);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int sid = 0;
            for (String sentence : value.toString().split("\n")) {

                // do nothing, if maximum number of documents has been seen
                if (--maxdocs < 0) {
                    return;
                }

                long did = key.hashCode() * 10000L + (long) sid++;;

                OpenObjectIntHashMap<String> wordCounts = new OpenObjectIntHashMap<String>();
                for (String term : sentence.split("\\s+")) {
                    wordCounts.adjustOrPutValue(term, +1, +1);
                }
                for (String term : wordCounts.keys()) {
                    outKey.set(term);
                    outValue.setDId(did);
                    outValue.setOffsets(new int[]{wordCounts.get(term)});
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static final class WordCountReducer extends Reducer<Text, PostingWritable, Text, Text> {

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        // singleton output value -- for efficiency reasons
        private final Text outValue = new Text();

        // collection frequencies
        private final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();

        // document frequencies
        private final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();

        @Override
        protected void reduce(Text key, Iterable<PostingWritable> values, Context context) throws IOException, InterruptedException {
            int cf = 0;
            int df = 0;
            for (PostingWritable value : values) {
                cf += value.getOffsets()[0];
                df++;
            }
            cfs.put(key.toString(), cf);
            dfs.put(key.toString(), df);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // sort terms in descending order of their collection frequency
        	List<String> temp = cfs.keys();
            String[] terms = (String[]) temp.toArray();
            Arrays.sort(terms, new Comparator<String>() {

                @Override
                public int compare(String t, String u) {
                    return cfs.get(u) - cfs.get(t);
                }
            });

            // assign term identifiers
            OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();
            for (int i = 0; i < terms.length; i++) {
                tids.put(terms[i], (i + 1));
            }

            // sort terms in lexicographic order and produce output
            Arrays.sort(terms);
            for (String term : terms) {
                outKey.set(term);
                outValue.set(cfs.get(term) + "\t" + dfs.get(term) + "\t" + tids.get(term));
                context.write(outKey, outValue);
            }
        }
    }

    //////
    /////
    //// PHASE 2: Transform document collection into integer sequences
    ///
    //
    public static final class TransformationMapper extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {

        // singleton output key -- for efficiency reasons
        private final LongWritable outKey = new LongWritable();

        // singleton output value -- for efficiency reasons
        private final IntArrayWritable outValue = new IntArrayWritable();

        // mapping from terms to their corresponding term identifiers
        private final OpenObjectIntHashMap<String> termTIdMap = new OpenObjectIntHashMap<String>();

        // maximum number of documents per file
        private int maxdocs;

        @Override
        protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
            try {
                maxdocs = context.getConfiguration().getInt("org.apache.mahout.fsm.maxdocs", Integer.MAX_VALUE);
                HashMap<String, String> dictionaryFiles = new HashMap<String, String>();
                for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                    if (cachedPath.toString().contains("wc") && cachedPath.toString().contains("part")) {
                        String file = cachedPath.toString();
                        dictionaryFiles.put(file.substring(file.lastIndexOf("/")), file);
                    }
                }
                ArrayList<String> fileNames = new ArrayList<String>(dictionaryFiles.keySet());
                Collections.sort(fileNames);
                for (String fileName : fileNames) {
                    BufferedReader br = new BufferedReader(new FileReader(dictionaryFiles.get(fileName)));
                    while (br.ready()) {
                        String[] tokens = br.readLine().split("\t");
                        termTIdMap.put(tokens[0], Integer.parseInt(tokens[3]));
                    }
                    br.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            int sid = 0;
            for (String sentence : value.toString().split("\n")) {

                // do nothing, if maximum number of documents has been seen
                if (--maxdocs < 0) {
                    return;
                }

                IntArrayList tids = new IntArrayList();
                for (String term : sentence.split("\\s+")) {
                    tids.add(termTIdMap.get(term));
                }

                long did = key.hashCode() * 10000L + (long) sid++;

                outKey.set(did);
                outValue.setContents(tids.toArray(new int[0]));
                context.write(outKey, outValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // read job parameters from commandline arguments
        String input = args[0];
        String output = args[1];

        // parse optional parameters        
        int maxdocs = (args.length > 2 ? Integer.parseInt(args[2]) : Integer.MAX_VALUE);
        maxdocs = (maxdocs == 0 ? Integer.MAX_VALUE : maxdocs);

        // delete output directory if it exists
        FileSystem.get(getConf()).delete(new Path(args[1]), true);

        ///
        /// PHASE 1: Compute word counts
        ///
        Job job1 = new Job(getConf());

        // set job name and options
        job1.setJobName("document collection conversion (phase 1)");
        job1.getConfiguration().setInt("org.apache.mahout.fsm.maxdocs", maxdocs);
        job1.setJarByClass(this.getClass());

        // set input and output paths
        FileInputFormat.setInputPaths(job1, DfsUtils.traverse(new Path(input), job1.getConfiguration()));
        TextOutputFormat.setOutputPath(job1, new Path(output + "/wc"));

        // set input and output format        
       
        job1.setInputFormatClass(TextInputFormat.class);
       
        job1.setOutputFormatClass(TextOutputFormat.class);

        // set mapper and reducer class
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);

        // set number of reducers
        job1.setNumReduceTasks(1);

        // map output classes
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(PostingWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // start job
        job1.waitForCompletion(true);

        ///
        /// PHASE 2: Transform document collection
        ///
        Job job2 = new Job(getConf());

        // set job name and options
        job2.setJobName("document collection conversion (phase 2)");
        job2.getConfiguration().setInt("org.apache.mahout.fsm.maxdocs", maxdocs);
        job2.setJarByClass(this.getClass());

        // set input and output paths
        FileInputFormat.setInputPaths(job2, DfsUtils.traverse(new Path(input), job2.getConfiguration()));
        SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/raw"));
        SequenceFileOutputFormat.setCompressOutput(job2, false);

        // set input and output format
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper and reducer class
        job2.setMapperClass(TransformationMapper.class);

        // set number of reducers
        job2.setNumReduceTasks(1);

        // map output classes
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(IntArrayWritable.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(IntArrayWritable.class);

        // add files to distributed cache
        for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(output + "/wc"))) {
            if (file.getPath().toString().contains("part")) {
                DistributedCache.addCacheFile(file.getPath().toUri(), job2.getConfiguration());
            }
        }

        // start job
        job2.waitForCompletion(true);
        

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConvertTextSentences(), args);
        System.exit(exitCode);
    }
}
