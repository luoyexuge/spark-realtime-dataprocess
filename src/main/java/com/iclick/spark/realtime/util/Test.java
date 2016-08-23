package com.iclick.spark.realtime.util;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
public class Test {
	public static void main(String[] args) throws UnsupportedEncodingException {
	  String  test="%E5%80%A9%E7%A2%A7%E9%BB%84%E6%B2%B9%E7%9A%84%E6%AD%A3%E7%A1%AE%E7%94%A8%E6%B3%95";
	  String test_decode=URLDecoder.decode(test,"UTF-8");
	  System.out.println(test_decode);
		
	  
	  String   test_encode=URLEncoder.encode("倩碧黄油的正确用法","UTF-8");
	  System.out.println(test_encode);
	  
	  /*result:
		  倩碧黄油的正确用法
		  %E5%80%A9%E7%A2%A7%E9%BB%84%E6%B2%B9%E7%9A%84%E6%AD%A3%E7%A1%AE%E7%94%A8%E6%B3%95
*/
		
		
	}

}
