package com.teamsun.metadata;

public class aaa {
	public static void main(String[] args) {
//		String abc = "||1|2||3|4";
		String abc = "1|+|2|+||+||+||+||+|";
		/*		for (String a : abc.split("\\|\\+\\|",9)) {
			System.out.println(a.equals("")?"kong":a);
		}
//		System.out.println(repairSeparator(abc.replaceAll("\\|\\+\\|", "")));
System.out.println(abc.split("\\|\\+\\|",9).length);*/
//		System.out.println(abc.split("\\|").length);
		System.out.println(abc.split("\\|\\+\\|",10).length);
//		System.out.println(abc.split("\\|\\+\\|")[3]);
		
	}
	public static String repairSeparator(String value){
		if(value.contains("+|")){
			value = value.replaceAll("\\+\\|", "");
		}else if(value.contains("|+")){
			value = value.replaceAll("\\|\\+", "");
		}
		return value;
	}
}
