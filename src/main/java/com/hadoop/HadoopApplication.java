package com.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HadoopApplication {

	private static FileSystem fs = null;
	static{
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(new URI("hdfs://master:9000"), conf,"root");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 上传本地文件到hdfs服务器
	 * @param srcPath		本地路径 例如:"D:\\hello.txt"
	 * @param dstPath		服务器路径 例如:"/tmp"
	 * @throws Exception
	 */
	public static void put(String srcPath,String dstPath)throws Exception{
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		fs.copyFromLocalFile(src, dst);
		fs.close();
	}
	/**
	 * 删除hdfs服务器上的文件(文件或文件夹)
	 * @param dstPath 服务器路径
	 * @throws Exception
	 */
	public static void delete(String dstPath) throws Exception{
		fs.delete(new Path(dstPath),true);
		fs.close();
	}
	/**
	 * 创建hdfs服务器上的路径
	 * @param dstPath
	 * @throws Exception
	 */
	public static void mkdir(String dstPath) throws Exception{
		fs.mkdirs(new Path(dstPath));
		fs.close();
	}
	/**
	 * 重命名文件名(文件或文件夹)
	 * @param src	原文件名
	 * @param dst	更改后的文件名
	 * @throws Exception
	 */
	public static void rename(String src,String dst) throws Exception{
		fs.rename(new Path(src), new Path(dst));
		fs.close();
	}
	/**
	 * 查看目录列表信息
	 * @throws Exception
	 */
	public static void lsFiles() throws Exception{
        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/hello"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------分割线--------------");
        }
	}
	/**
     * 查看文件及文件夹信息
     */
    public static void listAll() throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/tmp"));
        String flag = "d--             ";

        for (FileStatus fstatus : listStatus) {
//            if (fstatus.isFile())  
//                flag = "f--         ";
            System.out.println(flag + fstatus.getPath().getName());
        }
        fs.close();
    }
   /**
    * 读取分布式hdfs系统文件内容
    * @param dstPath	hdfs文件系统路径
    * @return	返回系统文件内容
    * @throws Exception
    */
    public static String cat(String dstPath) throws Exception{
    	Path path = new Path(dstPath);
    	String str = "";
    	if(fs.exists(path)){
    		FSDataInputStream is = fs.open(path);
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    		String info = null;
    		while((info=br.readLine())!=null){
    			str += info;
    		}
    		is.close();
    	}
    	return str;
    }
    /**
     * 下载服务器文件到本地
     * @param dfsPath	本地路径
     * @param srcPath	hdfs服务器路径
     * @throws Exception
     */
    public static void downLoad(String srcPath,String dfsPath) throws Exception{
    	Path dfs = new Path(dfsPath);
    	Path src = new Path(srcPath);
    	fs.copyToLocalFile(false,src, dfs,true);
    	fs.close();
    }
	public static void main(String[] args) throws Exception{
//		put("C:\\Users\\Administrator\\Desktop\\刚坯切割图形.sql","/hello");
//		listAll();
//		cat("/tmp/刚坯切割图形.sql");
		downLoad("/tmp/刚坯切割图形.sql","/hello/祝明.sql");
	}
}
