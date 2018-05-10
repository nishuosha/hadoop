package com.mtdp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        //hadoop 数据类型 IntWritable 初始化为1，代表了每个字符串的数量
        private final static IntWritable one = new IntWritable(1);
        //hadoop数据类型 Text， 分割后输出的字符串
        private Text word = new Text();

        //map的实现
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Java自带字符串分割类，默认分割符：空格, \t, \n, \r
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                //得到分割后的一个字符串并赋值给word
                word.set(itr.nextToken());
                //输出结果到reduce
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum = sum + val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.out.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        //获得job实例
        Job job = Job.getInstance(conf, "word count");
        //设置运行主类
        job.setJarByClass(WordCount.class);
        //设置mapper类
        job.setMapperClass(TokenizerMapper.class);
        //设置组合类
        job.setCombinerClass(IntSumReducer.class);
        //设置reduce类
        job.setReducerClass(IntSumReducer.class);
        //设置输出key类型
        job.setOutputKeyClass(Text.class);
        //设置输出value类型
        job.setOutputValueClass(IntWritable.class);
        //设置输入文件路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/wordcountdemo/input/wc1.input"));
        //设置输出文件夹路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
