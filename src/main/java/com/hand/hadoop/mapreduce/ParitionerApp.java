package com.hand.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用MapReduce开发ParitionerApp
 */
public class ParitionerApp {
    /**
     * 定义Driver：封装MapReduce作业的所有信息
     */
    public static void main(String[] args) throws Exception {
        //创建configuration
        Configuration configuration = new Configuration();
        //删除已存在的文件
        Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println("output file exists ,but has been deleted!");
        }

        //创建job
        Job job = Job.getInstance(configuration, "wordcount");
        //设置job的处理类
        job.setJarByClass(ParitionerApp.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置map相关参数
        job.setMapperClass(MyMpper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //通过job设置combiner处理类，其实逻辑上和我们的reduce是一模一样的
        //job.setCombinerClass(MyReducer.class);

        //设置job的partition
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个Reduer，每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*
    *Map:读取文件
    * */
    public static class MyMpper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接收每一行数据
            String line = value.toString();
            //按照指定分隔符进行拆分
            String[] words = line.split(" ");
            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
        }
    }

    /*
    *Reduce:归并操作
    * */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                //求key出现次数总和
                sum += value.get();
            }
            //最终统计结果的输出
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     *
     */
    public static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable longWritable, int i) {
            if (key.toString().equals("huawei")) {
                return 0;
            }
            if (key.toString().equals("xiaomi")) {
                return 1;
            }
            if (key.toString().equals("iphone7")) {
                return 2;
            }
            return 3;
        }
    }
}
