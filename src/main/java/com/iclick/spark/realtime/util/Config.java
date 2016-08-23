package com.iclick.spark.realtime.util;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


public class Config {
	private static  Logger   logger=Logger.getLogger(Config.class);
	private static Properties p=new Properties();
	private static String classPath = Config.class.getClassLoader().getResource("").getPath();
	private static String paramsFile = "config.properties";
	
	
	static{
		try{
		File file=new File(classPath+paramsFile);
		logger.info(file.getAbsolutePath());
		p.load(new FileInputStream(file));
		}catch(Exception  e){
			logger.error(e.getMessage(),e);
		}
	}
	
	
	public  static  String  getString(String  key){
		return p.getProperty(key);
	}
	
	public  static  String  getString(String key,String str){
		return p.getProperty(key,str);
	}
	public static Boolean getBoolean(String key){
		return getBoolean(key,false);
	}
	
	public static Boolean getBoolean(String key,Boolean defaultValue){
		String value = p.getProperty(key,String.valueOf(defaultValue));
		if(StringUtils.isBlank(value)){
			return false;
		}else{
			return Boolean.parseBoolean(value);
		}
	}
	public static int getInt(String key){
		return getInt(key,0);
	}
	
	public static int getInt(String key,int defaultValue){
		String value = p.getProperty(key,String.valueOf(defaultValue));
		if(StringUtils.isBlank(value)){
			return 0;
		}else{
			return Integer.parseInt(value);
		}
	}
	public static float getFloat(String key){
		return getFloat(key, 0);
	}
	
	public static float getFloat(String key,float defaultValue){
		String value = p.getProperty(key,String.valueOf(defaultValue));
		if(StringUtils.isBlank(value)){
			return 0;
		}else{
			return Float.parseFloat(value);
		}
	}
	
	public static double getDouble(String key){
		return getDouble(key, 0);
	}
	
	public static double getDouble(String key,double defaultValue){
		String value = p.getProperty(key,String.valueOf(defaultValue));
		if(StringUtils.isBlank(value)){
			return 0;
		}else{
			return Double.parseDouble(value);
		}
	}
	
	

	public static String getClassPath(){
		return classPath;
	}
	
	
	
	public static void main(String[] args) {
		logger.info("主程序开始");
		logger.error("报错");
		logger.info(getClassPath());
		logger.info(getString("mysql.driver"));
	}
	
}
