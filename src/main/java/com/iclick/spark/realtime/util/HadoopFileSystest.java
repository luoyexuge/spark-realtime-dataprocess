package com.iclick.spark.realtime.util;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class HadoopFileSystest {

	public static void writefile(String str) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("d:/tt.txt");
		if (fs.exists(path)) {
			fs.delete(path);
			System.out.println("删除成功");

		}

		FSDataOutputStream out = fs.create(path);
		int i = 0;
		while (i < 100) {
			byte[] by = str.getBytes();
			out.write(by);

			i += 1;
		}
		System.out.println("写完。。。");
		out.close();
		fs.close();

	}

	public static void readfile(String str)throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path=new Path(str);
	
       FSDataInputStream  	in=fs.open(path);
       BufferedReader  buff = new BufferedReader(new InputStreamReader(in));
       String str1 = buff.readLine();
       while (str1 != null){
         System.out.println(str1);
         str1 = buff.readLine();
       }
		
		
		
	}
	

	public static void main(String[] args) throws Exception {
		String str = "0.332" + "," + "0.69" + "\n";
		writefile(str);
		readfile("D:\\filwrite.txt");
		
		
	}
}