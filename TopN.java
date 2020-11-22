import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(" ");
            Text word = new Text(tokens[0]);
            String[] docs = tokens[1].split(";");
            int count = 0;
            for(int i =0; i<docs.length; i++){
                String[] file = docs[i].split(":");
                count += Integer.valueOf(file[1]);
            }
            context.write(word, new IntWritable(count));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private TreeMap<Integer, String> tmap;
        private long debug = 0;
        private int tsize = 0;
        private Set<String> stop;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap = new TreeMap<Integer, String>();
            stop = new HashSet<String>();
            //This is the stop word list
            stop.add("the");
            stop.add("a");
            stop.add("of");
            stop.add("and");
            stop.add("to");
            stop.add("in");
            stop.add("he");
            stop.add("his");
            stop.add("it");
            stop.add("was");
            stop.add("that");
            stop.add("with");
            stop.add("which");
            stop.add("had");
            stop.add("is");
            stop.add("you");
            stop.add("at");
            stop.add("on");
            stop.add("i");
            stop.add("this");
            stop.add("not");
            stop.add("as");
            stop.add("her");
            stop.add("an");
            stop.add("be");
            stop.add("by");
            stop.add("are");
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int num = Integer.valueOf(conf.get("num"));
            String word = key.toString();
            int count = 0;
            for (IntWritable val : values)
            {
                count += val.get();
            }
            if(!stop.contains(word)) {
                tmap.put(count, word);
    //                tsize++;
            }
            // we remove the first key-value
            // if it's size increases 10
            if (tmap.size() > num)
            {
                tmap.remove(tmap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer key : tmap.descendingKeySet()) {
                String w = tmap.get(key);
                context.write(new Text(w), new IntWritable(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("num", args[2]); //pass the int as a param
        Job job = Job.getInstance(conf, "TopN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1); //set the number of reducer to 1
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}