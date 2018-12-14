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
	 * �ϴ������ļ���hdfs������
	 * @param srcPath		����·�� ����:"D:\\hello.txt"
	 * @param dstPath		������·�� ����:"/tmp"
	 * @throws Exception
	 */
	public static void put(String srcPath,String dstPath)throws Exception{
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		fs.copyFromLocalFile(src, dst);
		fs.close();
	}
	/**
	 * ɾ��hdfs�������ϵ��ļ�(�ļ����ļ���)
	 * @param dstPath ������·��
	 * @throws Exception
	 */
	public static void delete(String dstPath) throws Exception{
		fs.delete(new Path(dstPath),true);
		fs.close();
	}
	/**
	 * ����hdfs�������ϵ�·��
	 * @param dstPath
	 * @throws Exception
	 */
	public static void mkdir(String dstPath) throws Exception{
		fs.mkdirs(new Path(dstPath));
		fs.close();
	}
	/**
	 * �������ļ���(�ļ����ļ���)
	 * @param src	ԭ�ļ���
	 * @param dst	���ĺ���ļ���
	 * @throws Exception
	 */
	public static void rename(String src,String dst) throws Exception{
		fs.rename(new Path(src), new Path(dst));
		fs.close();
	}
	/**
	 * �鿴Ŀ¼�б���Ϣ
	 * @throws Exception
	 */
	public static void lsFiles() throws Exception{
        // ˼����Ϊʲô���ص�������������List֮�������
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
            System.out.println("--------------�ָ���--------------");
        }
	}
	/**
     * �鿴�ļ����ļ�����Ϣ
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
    * ��ȡ�ֲ�ʽhdfsϵͳ�ļ�����
    * @param dstPath	hdfs�ļ�ϵͳ·��
    * @return	����ϵͳ�ļ�����
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
     * ���ط������ļ�������
     * @param dfsPath	����·��
     * @param srcPath	hdfs������·��
     * @throws Exception
     */
    public static void downLoad(String srcPath,String dfsPath) throws Exception{
    	Path dfs = new Path(dfsPath);
    	Path src = new Path(srcPath);
    	fs.copyToLocalFile(false,src, dfs,true);
    	fs.close();
    }
	public static void main(String[] args) throws Exception{
//		put("C:\\Users\\Administrator\\Desktop\\�����и�ͼ��.sql","/hello");
//		listAll();
//		cat("/tmp/�����и�ͼ��.sql");
		downLoad("/tmp/�����и�ͼ��.sql","/hello/ף��.sql");
	}
}
