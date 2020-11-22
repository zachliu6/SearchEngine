import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Searching {

    /*
    Since this is asked to be implemented on the cloud
    There's no need for a reducer function becauce essentially it's just
    looking for a specific string, and only one line (key value pair) will be returned
     */

    //static String term;
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String term = conf.get("term");
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String[] tokens = value.toString().split(" ");
            if(tokens[0].contains(term)){
                String[] docs = tokens[1].split(";");
                for(int i =0; i< docs.length; i++){
                    String[] result = docs[i].split(":");
                    context.write(new Text(result[0]), new IntWritable(Integer.valueOf(result[1])));
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("term", args[2]); //pass the term as a param
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        Job job = Job.getInstance(conf, "Searching");
        job.setJarByClass(Searching.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class); Not needed
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1); // This is important because we don't want more than one output file
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
