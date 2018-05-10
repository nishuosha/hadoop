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
import java.util.Arrays;

public class Second {

    public static class SecondMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            String friend = str[0];
            String[] users = str[1].split(",");

            Arrays.sort(users);

            for(int i = 0 ; i < users.length ; i++) {
                for(int j = i + 1; j < users.length ; j++) {
                    context.write(new Text(users[i] + "-" + users[j]), new Text(friend));
                }
            }
        }
    }

    public static class SecondReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for(Text t : values) {
                sb.append(t.toString()).append(" ");
            }

            context.write(key, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UserGroupSecond");

        job.setJarByClass(Second.class);
        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/UserGroup/output1/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.4.226.13:8081/UserGroup/output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
