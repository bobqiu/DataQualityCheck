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
public class DqcMapper_LJ extends Mapper<LongWritable, Text, Text, Text> {



	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sv = value.toString();
		matcher = pattern.matcher(sv);
		if(matcher.find())
			sv = sv.replace("^[ ]*\n", "");		
//		mos.write(Constants.OUTPUTFILENAME, null, new Text(repairSeparator(replaceRF(sv))));
		//判断一行数据以-|-|结尾可以处理
		if(repairSeparator(this.replaceRF(sv)).contains("-|-|")){
			String[] items = repairSeparator(this.replaceRF(sv)).split("\\|\\-\\|");
			for(String str:items){
				mos.write(Constants.OUTPUTFILENAME, null, new Text(str));
			}
		}else{
		String[] items = this.replaceRF(sv).split("\001",Integer.MAX_VALUE);
		if (this.colnumSize == items.length) {
			mos.write(Constants.OUTPUTFILENAME, null, new Text(replaceRF(sv)));
			return;
			}
		if (this.colnumSize != items.length) {
		items = repairSeparator(this.replaceRF(sv)).split("\001",Integer.MAX_VALUE);
		}
		if (this.colnumSize != items.length) {
			mos.write(Constants.ERRORFILENAME, null, new Text(sv + Constants.LF),ErrorOutput+Constants.ERRORFILENAME);
			mos.write(Constants.ERRORFILENAME, null,new Text("[real:"+items.length+"],"+"[expected:"+this.colnumSize+"]"),ErrorOutput+Constants.ERRORFILENAME);
			return;
		}
		mos.write(Constants.OUTPUTFILENAME, null, new Text(repairSeparator(replaceRF(sv))));
	}
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
				.replaceAll(Constants.LINUX_LF, "")
				.replaceAll(Constants.LINUX_LF_2,"")
				.replaceAll(Constants.LINUX_LF_3,"")
				.replaceAll(Constants.RF, Constants.HIVE_RF)
				.replaceAll(Constants.HIVE_RF+Constants.ORACLE_NULL+Constants.HIVE_RF, Constants.HIVE_RF+Constants.HIVE_RF)
				.replaceAll(Constants.HIVE_RF+Constants.ORACLE_null+Constants.HIVE_RF, Constants.HIVE_RF+Constants.HIVE_RF);
	}

	String val ;
	String ErrorOutput;
	private int colnumSize;
	MultipleOutputs<Text, Text> mos;
	protected void setup(Context context) throws IOException,
			InterruptedException {
		colnumSize = Integer.parseInt(context.getConfiguration().get(Constants.COLNUMSIZE));
		mos = new MultipleOutputs<Text, Text>(context);
		ErrorOutput = context.getConfiguration().get("Error_output");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}	
}
