package com.teamsun.mapred;

/**
 * 
 */

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
public class DqcMapper_deli_utf8 extends Mapper<LongWritable, Text, Text, Text> {

	Pattern pattern = Pattern.compile("^[ ]*\n");
	Matcher matcher;
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sv = value.toString();
		this.matcher = this.pattern.matcher(sv);
		if(this.matcher.find())
			sv=leftTrim(sv);
		String[] items = replaceRF(sv).split(Constants.HIVE_RF,Integer.MAX_VALUE);
		if (this.colnumSize != items.length) {
			items=repairSeparator(this.replaceRF(sv));
		}
		if (this.colnumSize != items.length) {
			mos.write(Constants.ERRORFILENAME,  new Text(sv+ Constants.LF),null,ErrorOutput+Constants.ERRORFILENAME);
			mos.write(Constants.ERRORFILENAME, new Text("[real:"+items.length+"],"+"[expected:"+this.colnumSize+"]"),null,ErrorOutput+Constants.ERRORFILENAME);
			return;
		}
		val="";
		boolean isFirst = true;
		
		for(String seq :seqs){
			if(isFirst){
				val=items[Integer.parseInt(seq)];
				isFirst=false;
			}else {
				val+=Constants.HIVE_RF+items[Integer.parseInt(seq)];
			}
		}
		//主键是否为空判断
		if(!("0".equals(PrimaryKeyFlag))){
			String[] Primary_Order_Id = PrimaryKeyFlag.split(Constants.COMMA);
			for(String id : Primary_Order_Id){
				if("".equals(val.split("\001",Integer.MAX_VALUE)[Integer.parseInt(id)-1].trim())){
					mos.write(Constants.PRIMARYKEYNULLNAME, null, new Text(
							(val.replaceAll(Constants.HIVE_RF,Constants.RF_DUPLICATION) + Constants.LF) 
							    + " "
								+ "[the failed column is:" + (Integer.parseInt(id)) +"]," + "[fail data is:"+val.split("\001",Integer.MAX_VALUE)[Integer.parseInt(id)-1].trim()+"],"
							    +" [real:it's null ]," + "[expected: it's not null]"),ErrorOutput+Constants.PRIMARYKEYNULLNAME);
					break;
				}
			}			
		}
		
			if(judge_length(val)){
			mos.write(Constants.OUTPUTFILENAME, null, new Text(this.val));
			}	
		
	}
	public boolean judge_length(String col) throws IOException, InterruptedException{

		String[] DataLength = col.split("\001",Integer.MAX_VALUE);
		int i = 0;
		for(String dll:DataLength){
		int method_code = Integer.parseInt(col_data_length[i].substring(col_data_length[i].indexOf('+')+1));
		//FLOAT,BOLB,RAW,ROWID,UROWID,VARBINARY,BIT类型不校验
		if(method_code == 0){
			i++;
			continue;
		}
		//校验(CHAR,NCHAR,NVARCHAR,NVARCHAR,VARCHAR,VARCHAR2，alpha)验证数据长度---method=1
		if(method_code == 1){
		if (("".equals(col_data_length[i].substring(0, col_data_length[i].indexOf('+'))))
					|| (dll.length() <= Integer.parseInt(col_data_length[i]
							.substring(0, col_data_length[i].indexOf('+'))))) {
			i++;
			continue;
		}else {
				mos.write(Constants.ERRORDATAFILENAME, null, new Text(
							(col.replaceAll(Constants.HIVE_RF,
										Constants.RF_DUPLICATION) + Constants.LF) + " "
								+ "[the failed column is:" + (i+1) +"]," + "[fail data is:"+dll+"],"+" [real:"
								+ dll.length()+"]," + "[expected:"
								+ col_data_length[i].substring(0, col_data_length[i].indexOf('+'))+"]"),ErrorOutput+Constants.ERRORDATAFILENAME);
			return false;
		}
		}

		//NUMBER(p,s)---method=2
		//精度，刻度校验；数字类型长度校验，整数数字类型字符类型匹配
		if(method_code == 2){
			//判断number(p,s)提供精度及刻度的
			if(col_data_length[i].substring(0, col_data_length[i].indexOf('+')).contains("(")){
				int p = Integer.parseInt(col_data_length[i].substring(0, col_data_length[i].indexOf('+')).substring(1,col_data_length[i].indexOf(',')));
				int s = Integer.parseInt(col_data_length[i].substring(0, col_data_length[i].indexOf('+')).substring(col_data_length[i].indexOf(',')+1,col_data_length[i].indexOf(')')));	
				Pattern pattern_1 = Pattern.compile("(^(-?)\\d{0,"+(p-s)+"}\\.\\d{0,"+s+"}$)|(^(-?)\\d{0,"+(p-s)+"}$)");
				if(pattern_1.matcher(dll).find()){
					i++;
					continue;
				}else{
					mos.write(Constants.ERRORDATAFILENAME, null, new Text(
							(col.replaceAll(Constants.HIVE_RF,
										Constants.RF_DUPLICATION) + Constants.LF) + " "
								+ "[the failed column is:" + (i+1) +"]," + "[fail data is:"+dll+"]," + "[expected:"
								+ col_data_length[i].substring(0, col_data_length[i].indexOf('+'))+"]"),ErrorOutput+Constants.ERRORDATAFILENAME);
			return false;
				}
				
			}
			//DDL中未提供字段长度，则跳过
			else if("".equals(col_data_length[i].substring(0, col_data_length[i].indexOf('+')))){
				i++;
				continue;
			    }
			//整数类型(包含有符号类型)匹配，如：number类型字段长度不包含刻度，(INT,INTEGER,NUMBER(p),SMALLINT,TINYINT,)
			else{
				int p =Integer.parseInt(col_data_length[i].substring(0, col_data_length[i].indexOf('+')));
				Pattern pattern_2 = Pattern.compile("^(-?)\\d{0,"+p+"}$");
				if(pattern_2.matcher(dll).find()){
					i++;
					continue;
				}else{
					mos.write(Constants.ERRORDATAFILENAME, null, new Text(
							(col.replaceAll(Constants.HIVE_RF,
										Constants.RF_DUPLICATION) + Constants.LF) + " "
								+ "[the failed column is:" + (i+1) +"]," + "[fail data is:"+dll+"]," + "[expected:"
								+ col_data_length[i].substring(0, col_data_length[i].indexOf('+'))+"]"),ErrorOutput+Constants.ERRORDATAFILENAME);
			return false;
				}			
			}						
	    }
		
		//日期格式匹配校验(DATE,DATETIME)
		//时间戳格式匹配(TIMESTAMP)
		if(method_code == 3){
			Pattern pattern_3 = Pattern.compile("(^\\d{8}$)|(^\\d{2}-\\d{2}-\\d{4}$)" +
					"|(^\\d{4}-\\d{1,2}-\\d{1,2}$)|(^\\d{4}.\\d{1,2}.\\d{1,2}$)" +
					"|(^\\d{4}/\\d{1,2}/\\d{1,2}$)|(^\\d{14}$)" +
					"|(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$)" +
					"|(^\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}$)" +
					"|(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}$)" +
					"|(^\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}$)" +
					"|(^\\d{8} \\d{2}:\\d{2}:\\d{2}:\\d{6}$)" +
					"|^14\\d{8}$" +
					"|^$");
			if(!pattern_3.matcher(dll).find()){
				mos.write(Constants.DATEFORMATNAMEERROR, null, new Text(
						(col.replaceAll(Constants.HIVE_RF,
									Constants.RF_DUPLICATION) + Constants.LF) + " "
							+ "[the failed column is:" + (i+1) +"]," + "[fail data is:"+dll+"]," + "[expected:"
							+ col_data_length[i].substring(0, col_data_length[i].indexOf('+'))+"]"),ErrorOutput+Constants.DATEFORMATNAMEERROR);
			}
			i++;
			continue;
		}	
		}
		return true;
	
	}
	
	public String[] repairSeparator(String value){
		if(value.contains("*|")){
			value = value.replaceAll("\\*\\|", Constants.HIVE_RF);
			String[] items = value.split(Constants.HIVE_RF, Integer.MAX_VALUE);
			if(colnumSize == items.length )
				return items;
		}else if(value.contains("|*")){
			value = value.replaceAll("\\|\\*", Constants.HIVE_RF);
			String[] items = value.split(Constants.HIVE_RF, Integer.MAX_VALUE);
			if(colnumSize == items.length )
				return items;
		}
//		return new String[0];
		return value.split(Constants.HIVE_RF, Integer.MAX_VALUE);
	}
	
		
	/**
	 * 
	 * @param value
	 * @return
	 */
	public static String leftTrim(String str){
		return str.replaceAll("^[ ]*", "");
	}
	public String replaceRF(String value){
		return value.replaceAll("\020", "")
				.replaceAll("\001", "")
				.replaceAll(Constants.LINUX_LF, "")
				.replaceAll(Constants.LINUX_LF_2, "")
				.replaceAll(Constants.RF_DUPLICATION, Constants.HIVE_RF)
				.replaceAll(Constants.HIVE_RF+Constants.ORACLE_NULL+Constants.HIVE_RF, Constants.HIVE_RF+Constants.HIVE_RF)
				.replaceAll(Constants.HIVE_RF+Constants.ORACLE_null+Constants.HIVE_RF, Constants.HIVE_RF+Constants.HIVE_RF);
	}

	String val ;
	String ErrorOutput;
	String PrimaryKeyFlag;
	private int colnumSize;
	MultipleOutputs<Text, Text> mos;
	private String[] seqs;
	private String[] col_data_length;
	protected void setup(Context context) throws IOException,
			InterruptedException {
		colnumSize = Integer.parseInt(context.getConfiguration().get(Constants.COLNUMSIZE));
		mos = new MultipleOutputs<Text, Text>(context);
		ErrorOutput = context.getConfiguration().get("Error_output");
		seqs = context.getConfiguration().get("colSeq").split(Constants.COMMA);
		col_data_length = context.getConfiguration().get("colData_length").split(Constants.COMMA_1);
		PrimaryKeyFlag = context.getConfiguration().get("PrimaryFlag");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
}
