import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndex{

    /*
    mapper class for the Inverted index algorithm
    extended on the basic mapreduce code here:
    https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
    it stores each word(key) and the docID(value) it appeared in
    */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String docID;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            docID = ((FileSplit) context.getInputSplit()).getPath().getName(); //get the name of current file
            String line = value.toString();
            //line = line.replaceAll("[^a-z]", ""); //delete all the non-alphabetical characters
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z]","").toLowerCase());
                context.write(word, new Text (docID));
            }
        }
    }


    /*
     reducer class for the Inverted index algorithm
     extended on the basic mapreduce code here:
     https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
     for every key(word), it merges docID with according frequencies into a post list(value)
    */
    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//            hmap = new HashMap<String, Integer>();
//        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //private StringBuilder sb; //use a stringbuilder to make the post listing for final output
            //use a hashmap to store <docID, freq> for each word
            HashMap<String, Integer> hmap = new HashMap<String, Integer>(); //initialize the hashmap
            StringBuilder sb = new StringBuilder();//use a stringbuilder to make the post listing for final output
            int sum = 0;
            //String word = key.toString();
            for (Text val : values) {
                if(hmap.containsKey(val.toString())){
                    hmap.put(val.toString(), (hmap.get(val.toString()) + 1));
                }else {
                    hmap.put(val.toString(), 1);
                }
            }
            //here every docID that this word appeared in is stored in the hashmap
            //just iterate through the hashmap and merge them into a post listing
            for (String docID : hmap.keySet()){
                sb.append( docID+ ":"+ hmap.get(docID) + ";"); //post listing: "DOCNAME:1;DOCNAME2:2;..."
            }
            context.write(key, new Text(sb.toString()));

            //context.write(key, new Text(hmap.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "Inverted Index Job");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        //I spent 2 hours looking for a bug and it was because of this line LOL
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}