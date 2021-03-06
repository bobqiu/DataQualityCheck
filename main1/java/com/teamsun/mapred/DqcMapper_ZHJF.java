/**
 * 
 */
package com.teamsun.mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.teamsun.common.Constants;

/**
 * @author wpf
 *
 */
public class DqcMapper_ZHJF extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sv = value.toString();
		matcher = pattern.matcher(sv);
		if(matcher.find())
			sv = sv.replace("^[ ]*\n", "");		
		mos.write(Constants.OUTPUTFILENAME, null, new Text(replaceRF(sv)));
	}
	
	public static String leftTrim(String str){
		return str.replaceAll("^[ ]*", "");
	}
	Pattern pattern = Pattern.compile("^[ ]*\n");
	Matcher matcher;
	public String repairSeparator(String value){
		if(value.contains("+|")){
			value = value.replaceAll("\\+\\|", Constants.HIVE_RF);
		}else if(value.contains("|+")){
			value = value.replaceAll("\\|\\+", Constants.HIVE_RF);
		}
		return value;
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	public String replaceRF(String value){
		return value.replaceAll("\020", "")
				.replaceAll("\001", "")
				.replaceAll("\\|", Constants.HIVE_RF);
	}

	String val ;
	private int colnumSize;
	MultipleOutputs<Text, Text> mos;
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
/*	public static void main(String[] args) {
		String str ="/111/111/11";
		System.out.println(str.split("/")[str.split("/").length-1]);
	}*/
	
}
