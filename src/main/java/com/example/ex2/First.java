package com.example.ex2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class First {

    public static class FirstMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] str = value.toString().split(":");
            String user = str[0];
            String[] friends = str[1].split(",");
            for(String s : friends) {
                context.write(new Text(s), new Text(user));
            }

        }
    }

    public static class FirstReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for(Text t : values) {
                sb.append(t.toString()).append(",");
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UserGroupFirst");

        job.setJarByClass(First.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/UserGroup/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.4.226.13:8081/UserGroup/output1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


