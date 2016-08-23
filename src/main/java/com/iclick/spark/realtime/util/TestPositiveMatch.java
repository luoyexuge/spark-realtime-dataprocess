package com.iclick.spark.realtime.util;

import java.util.Set;
import java.util.HashSet;

public class TestPositiveMatch {
	private String inputStr = null;
	private Set<String> dict = new HashSet<String>();
	public StringBuffer outputStr = new StringBuffer();

	public TestPositiveMatch(String inputStr) {
		this.inputStr = inputStr;
	}

	private void creatediction() {
		dict.add(" ");
		dict.add("爱");
		dict.add("中华");
		dict.add("中华人民共和国");
	}

	private void wordMathchString() {
		String inputString = inputStr;
		int strLen = inputString.length();
		int j = 0;

		int matchPos = 0;
		
		while (j < strLen) {

			String tempString = inputString.substring(j);
			int templen = tempString.length();
			int i = 1;
			String matchWordString = null;
			while (i <= templen) {

				String keyTemp = tempString.substring(0, i);

				if (dict.contains(keyTemp)) {
					matchWordString = keyTemp;
					matchPos = i;
				}
				i += 1;
			}
			if (matchWordString == null) {
				outputStr.append(tempString.substring(0,  1)).append("/");
				j += 1;
			} else {
				outputStr.append(matchWordString).append("/");

				j +=matchPos;
			}
		}

	}

	public void WordMatch() {
		creatediction();
		wordMathchString();
	}

	public static void main(String[] args) {
		String str = "我爱中华人民共和国";
//		String str = "我爱中华正度是多少大大";
		TestPositiveMatch pos = new TestPositiveMatch(str);
		pos.WordMatch();
		System.out.println(pos.outputStr.toString());

//		System.out.println(str.substring(2));

	}

}
