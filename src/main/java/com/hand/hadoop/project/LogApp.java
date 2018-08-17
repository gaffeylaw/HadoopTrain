package com.hand.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用MapReduce来完成我们的需求：统计l浏览器访问次数
 */
public class LogApp {
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
        Job job = Job.getInstance(configuration, "LogApp");
        //设置job的处理类
        job.setJarByClass(LogApp.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置map相关参数
        job.setMapperClass(LogApp.MyMpper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(LogApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置作业处理输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*
    *Map:读取文件
    * */
    public static class MyMpper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        private UserAgentParser userAgentParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接收每一行数据：其实就是一行日志信息
            String line = value.toString();


            String source = line.substring(getChareacterPosition(line, "\"", 5) + 1, getChareacterPosition(line, "\"", 6));
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();
            String os = agent.getOs();
            String engine = agent.getEngine();
            String engineVersion = agent.getEngineVersion();
            String platform = agent.getPlatform();

            //通过上下文把map处理结果输出
            context.write(new Text(browser), one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
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
     * 获取指定字符串中指定标识的字符串出现的索引位置
     *
     * @param value    字符串的值
     * @param operator 匹配字符
     * @param index    匹配字符的位置
     * @return
     */
    private static int getChareacterPosition(String value, String operator, int index) {
        Matcher matcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (matcher.find()) {
            mIdx++;
            if (mIdx == index) {
                break;
            }
        }
        return matcher.start();
    }
}

