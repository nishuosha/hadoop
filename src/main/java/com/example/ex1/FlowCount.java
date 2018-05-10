package com.example.ex1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount  {

    public static class FlowMapper extends Mapper<Object, Text, Text, FlowBean> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" ");
            String phoneNum = fields[0];
            int upFlow = Integer.parseInt(fields[1]);
            int downFlow = Integer.parseInt(fields[2]);

            context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow) );
        }
    }

    public static class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            int sum_upFlow = 0;
            int sum_downFlow = 0;

            for(FlowBean bean : values) {
                sum_upFlow = sum_upFlow + bean.getUpFlow();
                sum_downFlow = sum_downFlow + bean.getDownFlow();
            }

            FlowBean result = new FlowBean(sum_upFlow, sum_downFlow);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FlowCount");

        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //自定义分区，将结果输出到不同的文件夹中
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://10.4.226.13:8081/FlowCountDemo/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.4.226.13:8081/FlowCountDemo/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }


}
