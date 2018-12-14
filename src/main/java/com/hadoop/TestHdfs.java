package com.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
public class TestHdfs {
	private static String dir = "/tmp/testhdfsdir";
    private static String targetFile = "/write";
    private static Configuration conf = new Configuration();

//    static {
//        conf.set("fs.default.name", "hdfs://www.kiven.host:9000");
//    }

    public static void main(String[] args) throws Exception {
//        writeFile();
//        readFile();
//    	deleteFile();
//    	listFile();
    }

    public static void writeFile() throws Exception {
        FileSystem fs = FileSystem.get(conf);

        //´´½¨Ä¿Â¼
        Path tmpPath = new Path(dir);
        if (!fs.exists(tmpPath)) {
            fs.mkdirs(tmpPath);
        }

        FSDataOutputStream outputStream = fs.create(new Path(dir + targetFile), true);

        for (int i = 0; i < 10; i++) {
            outputStream.writeBytes("this is's hadooop" + i + "\n");
        }
        outputStream.close();
    }

    public static void readFile() throws Exception {
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(dir + targetFile);
        FSDataInputStream inputStream = fs.open(path);

        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
        String info = null;
        while ((info = bf.readLine()) != null) {
            System.out.println(info);
        }

        inputStream.close();
    }
    
    public static void deleteFile() throws Exception{
    	FileSystem fs = FileSystem.get(conf);
    	Path tmpPath = new Path(dir+targetFile);
    	boolean flag = fs.deleteOnExit(tmpPath);
    	System.err.println(flag);
    	fs.close();
    }
    
    public static void listFile() throws Exception{
    	FileSystem fs = FileSystem.get(conf);
    	FileStatus[] fileStatus = fs.listStatus(new Path(dir));
    	for (FileStatus fileStatus2 : fileStatus) {
			System.err.println(fileStatus2.getPath());
		}
    	fs.close();
    }
}
