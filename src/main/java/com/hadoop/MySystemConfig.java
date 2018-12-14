package com.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;


public class MySystemConfig {
	private MySystemConfig(){}
	private static Properties pro=null;
	static{
		pro = new Properties();
		getProperties();
	}
	
	public static void getProperties(){
		InputStream stream = MySystemConfig.class.getClassLoader().getResourceAsStream("system-config.properties");
		try {
			pro.load(stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Iterator<Object>  iterator = pro.keySet().iterator();
		while(iterator.hasNext()){
			String key = (String)iterator.next();
			System.err.println("{"+key+"="+pro.getProperty(key)+"}");
		}
	}
	
	public static String[] getPropertyArray(String proKey,String splitStr){
		String key = (String)pro.get(proKey);
		if(key==null||key.equals("")){
			return null;
		}
		return key.split(splitStr);
	}
	
	public static String getProperty(String str){
		return pro.getProperty(str);
	}
	public static void main(String[] args) throws Exception{
		CharSequence chars = new String("str");
		String str = "oraadf";
		System.out.println();
	}
}
