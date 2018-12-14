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
    	 // ��Ϊ��ÿ�����ʳ��ֺ󣬾���Ϊ 1����������Ϊһ��<key,value>�ԣ���˿�������Ϊ������ֵΪ 1	VALUEOUT
        private final static IntWritable one = new IntWritable(1);
        // ������	KEYOUT
        private Text word = new Text();
        /**
         * value ���ı�ÿһ�е�ֵ 
         * context �������Ķ���
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	// �ִʣ���ÿ�еĵ��ʽ��зָ�,����"  \t\n\r\f"(�ո��Ʊ�������з����س�������ҳ)���зָ�
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	 // ��ȡÿ��ֵ	���� map ����� key ֵ
                word.set(itr.nextToken());
             // ��������� map ������
                context.write(word, one);
            }
        }
    }
    /** 
     * Reducer ����WordCount ���� Reduce ��
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:Map ��������ͣ�����Reduce ����������
     * @author johnnie
     *
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
    	// ���������ܴ���
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; // �ۼ������ۼ�ÿ�����ʳ��ֵ��ܴ���
            for (IntWritable val : values) {
                sum += val.get(); // �ۼ�
            }
            result.set(sum);// ������� value
            context.write(key, result);// ��������� reduce ���
        }
    }
 // Driver �����ͻ���
    public static void run(String[] args) throws Exception{
    	// ��ȡ������Ϣ
        Configuration conf = new Configuration();
        // ����һ�� Job
        Job job = Job.getInstance(conf, "te");// ���� job name Ϊ word count
        job.setJarByClass(WordCount.class);// 1. ���� Job ���е���
        job.setMapperClass(TokenizerMapper.class); // 2. ����Mapper���Reducer��
        job.setReducerClass(IntSumReducer.class); // 4. ���������� key �� value ������
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);
     // 3. ��ȡ������������������ļ�Ŀ¼������ļ�Ŀ¼
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
     // 5. �ύ job���ȴ����н�������ڿͻ�����ʾ������Ϣ������������
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
    	run(args);
    }

}