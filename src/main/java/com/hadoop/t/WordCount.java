package com.hadoop.t;
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
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
    	 // 因为若每个单词出现后，就置为 1，并将其作为一个<key,value>对，因此可以声明为常量，值为 1	VALUEOUT
        private final static IntWritable one = new IntWritable(1);
        // 输出结果	KEYOUT
        private Text word = new Text();
        /**
         * value 是文本每一行的值 
         * context 是上下文对象
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	// 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	 // 获取每个值	设置 map 输出的 key 值
                word.set(itr.nextToken());
             // 上下文输出 map 处理结果
                context.write(word, one);
            }
        }
    }
    /** 
     * Reducer 区域：WordCount 程序 Reduce 类
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:Map 的输出类型，就是Reduce 的输入类型
     * @author johnnie
     *
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
    	// 输出结果：总次数
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; // 累加器，累加每个单词出现的总次数
            for (IntWritable val : values) {
                sum += val.get(); // 累加
            }
            result.set(sum);// 设置输出 value
            context.write(key, result);// 上下文输出 reduce 结果
        }
    }
 // Driver 区：客户端
    public static void run(String[] args) throws Exception{
    	// 获取配置信息
        Configuration conf = new Configuration();
        // 创建一个 Job
        Job job = Job.getInstance(conf, "te");// 设置 job name 为 word count
        job.setJarByClass(WordCount.class);// 1. 设置 Job 运行的类
        job.setMapperClass(TokenizerMapper.class); // 2. 设置Mapper类和Reducer类
        job.setReducerClass(IntSumReducer.class); // 4. 设置输出结果 key 和 value 的类型
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);
     // 3. 获取输入参数，设置输入文件目录和输出文件目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
     // 5. 提交 job，等待运行结果，并在客户端显示运行信息，最后结束程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
    	run(args);
    }

}