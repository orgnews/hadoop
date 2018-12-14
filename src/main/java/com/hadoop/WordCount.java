package com.hadoop;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

	private static final Logger log = LoggerFactory.getLogger(WordCount.class);
	private static Configuration conf = null;
	static{
//		String[] host = MySystemConfig.getPropertyArray("cluster1.hdfs.host", ",");
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://cluster1");
		conf.set("dfs.nameservices", "cluster1");
		conf.set("dfs.da.namenodes.cluste1", "master,slave1");
		conf.set("dfs.namenode.rpc-address.cluster1.master", "master");
		conf.set("dfs.namenode.rpc-address.cluster1.slave1", "slave1");
		conf.set("dfs.client.failover.proxy.provider.cluster1","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/**
		 * Դ�ļ���a b b
		 * 
		 * map֮��
		 * 
		 * a 1
		 * 
		 * b 1
		 * 
		 * b 1
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());	// ���ж�ȡ
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());								// ���ո�ָ��
				context.write(word, one);								// ÿ��ͳ�Ƴ����ĵ���+1
			}
		}
	}

	/**
	 * reduce֮ǰ��
	 * 
	 * a 1
	 * 
	 * b 1
	 * 
	 * b 1
	 * 
	 * reduce֮��:
	 * 
	 * a 1
	 * 
	 * b 2
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();		// �����ۼ�
			}
			result.set(sum);
			context.write(key, result);	// ����ͬ��key���
		}
	}

	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				log.info("args length is 0");
				run("test");
			} else {
				run(args[0]);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private static void run(String name) throws Exception {
		Job job = Job.getInstance(conf);				// ����һ�������ύ����
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);	// ָ��Map�������
		job.setCombinerClass(IntSumReducer.class);	// �ϲ�����
		job.setReducerClass(IntSumReducer.class);	// Reduce����
		job.setOutputKeyClass(Text.class);			// ���Key����
		job.setOutputValueClass(IntWritable.class);	// ���ֵ����

		// ����ͳ���ļ��ڷֲ�ʽ�ļ�ϵͳ�е�·��
		String inPath = String.format("/root", name);
//		 �����������ڷֲ�ʽ�ļ�ϵͳ�е�·��
		String outPath = String.format("/hello/output", name);

		FileInputFormat.addInputPath(job, new Path(inPath));		// ָ������·��
		FileOutputFormat.setOutputPath(job, new Path(outPath));	// ָ�����·��

		int status = job.waitForCompletion(true) ? 0 : 1;

		System.exit(status);										// ִ����MR������˳�Ӧ��
	}
}
