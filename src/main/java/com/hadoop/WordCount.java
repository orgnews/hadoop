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
		 * 源文件：a b b
		 * 
		 * map之后：
		 * 
		 * a 1
		 * 
		 * b 1
		 * 
		 * b 1
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());	// 整行读取
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());								// 按空格分割单词
				context.write(word, one);								// 每次统计出来的单词+1
			}
		}
	}

	/**
	 * reduce之前：
	 * 
	 * a 1
	 * 
	 * b 1
	 * 
	 * b 1
	 * 
	 * reduce之后:
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
				sum += val.get();		// 分组累加
			}
			result.set(sum);
			context.write(key, result);	// 按相同的key输出
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
		Job job = Job.getInstance(conf);				// 创建一个任务提交对象
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);	// 指定Map计算的类
		job.setCombinerClass(IntSumReducer.class);	// 合并的类
		job.setReducerClass(IntSumReducer.class);	// Reduce的类
		job.setOutputKeyClass(Text.class);			// 输出Key类型
		job.setOutputValueClass(IntWritable.class);	// 输出值类型

		// 设置统计文件在分布式文件系统中的路径
		String inPath = String.format("/root", name);
//		 设置输出结果在分布式文件系统中的路径
		String outPath = String.format("/hello/output", name);

		FileInputFormat.addInputPath(job, new Path(inPath));		// 指定输入路径
		FileOutputFormat.setOutputPath(job, new Path(outPath));	// 指定输出路径

		int status = job.waitForCompletion(true) ? 0 : 1;

		System.exit(status);										// 执行完MR任务后退出应用
	}
}
