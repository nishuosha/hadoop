package com.mtdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class QuChong {

    public static class qMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while(tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new Text(""));
            }
        }
    }


    public static class qReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "quchong");

        job.setJarByClass(QuChong.class);
        job.setMapperClass(qMapper.class);
        job.setCombinerClass(qReduce.class);
        job.setReducerClass(qReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/wordcountdemo/input/wc.input"));
        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/wordcountdemo/input/wc1.input"));

        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.4.226.13:8081/demo/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
