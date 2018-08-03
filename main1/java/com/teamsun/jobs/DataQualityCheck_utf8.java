/**
 * 
 */
package com.teamsun.jobs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.common.Configure;
import com.teamsun.common.Constants;
import com.teamsun.common.DBUtil;
import com.teamsun.common.FileUtil;
import com.teamsun.common.JobUtil;
import com.teamsun.exception.TitleNotFoundException;
import com.teamsun.input.TextFormatForDQC;
import com.teamsun.mapred.DqcMapper_utf8;
import com.teamsun.metadata.TableService;
/**
 * @author wpf
 * 20160510
 *
 */
public class DataQualityCheck_utf8 {
	
	private static final Logger Log = LoggerFactory.getLogger(DataQualityCheck.class);
	private static String interfaceCode; 
	private static String inputPath;
	private static String outputPath;
	private static String ErroroutputPath;
	private static String dataTime;
	private static String sysCode;
	private static String title;
	
	/**
	 * /data/recv/99340000000/20150319/LOAN-TB_LON_LOAN/99340000000_LOAN-TB_LON_LOAN_0000_20150319_I_0001_0001.txt
	 */
	
	/**
	 * 1.检查字段是否发生变化
	 * 2.数据校验
	 * 3.生成DDL文件
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) {
		if(args.length!=3){
			usage("Wrong number of arguments: " + args.length);
			System.exit(-1);
		}
		interfaceCode = args[0];
		sysCode = args[1];
		dataTime = args[2];
		inputPath = Constants.BASE_INPUT_PATH+sysCode+"/"+dataTime+"/"+interfaceCode+"/";
		
		Log.info(String.format("dataTime [%s],interface [%s],input [%s]", dataTime,interfaceCode,inputPath));
		int status = -1;
		try {
			Configure.init();
			DBUtil.initDs();
			Configuration conf = new Configuration();
			TableService ts = new TableService();
			
			//从bd_int_fields_config中查询字段个数，如果是0，直接退出
			long col_Size = ts.countTableCols(interfaceCode,sysCode);
			if(col_Size == 0){
				Log.error("TableStruct not exist...");
				status=-1;
				System.exit(status);
			}
			
			String titleFileName= "";
			try{
		titleFileName=FileUtil.getTitleFileName(conf, new Path(inputPath));
			} catch (TitleNotFoundException e) {
				Log.info("title not find!");
				System.exit(status);
			}
						
			title = inputPath+titleFileName;
			outputPath = Constants.BASE_OUTPUT_PATH+sysCode+"/"+dataTime+"/"+interfaceCode+"/";
			ErroroutputPath = Constants.ERROR_BASE_OUTPUT_PATH+sysCode+"/"+dataTime+"/"+interfaceCode+"/";
			
			if(!ts.checkTabCol(interfaceCode, sysCode,title,conf)){
				status=-1;
				Log.error("Table Column has changed...");
				System.exit(status);
			}
			String seq = ts.getColSeq(conf,interfaceCode,sysCode,title);
			//删除目录下的title文件
			if(!FileUtil.delFile(conf,new Path(title),titleFileName)){
				status=-1;
				Log.error("Del file error...");
				System.exit(status);
			}
			//获取接口字段长度及校验方法
			String data_length = ts.getColData_length(interfaceCode,sysCode);
			//获取是主键的order_id
			String primarykey_order_id = ts.getColPrimary_order_id(interfaceCode, sysCode);
			status = new DataQualityCheck_utf8().run(col_Size,seq,data_length,primarykey_order_id,conf);
			
			//如果map执行完成后返回值为0，判断目标文件中是否有error文件，有了以非0退出，作业失败
			if(status==0){
				if(FileUtil.getErrorFileNameExists(conf,new Path(ErroroutputPath))){
					if(!FileUtil.getPrimaryNullFileName(conf,new Path(ErroroutputPath))){
						//若keynull文件存在，则更新数据库表primarykey_is_null表
						int sum_line = 0;
						sum_line = FileUtil.readFile2List(conf, ErroroutputPath);
						
						ts.delete_pm_is_null(sysCode, interfaceCode, dataTime);
						ts.insert_pm_is_null(sysCode, interfaceCode, sum_line, dataTime);
					}
				if(!FileUtil.getErrorFileName(conf,new Path(ErroroutputPath))){
					status=-1;
					Log.error("errorColLength or errorDataLength file exist...");
					System.exit(status);
				}
				}
			}
		} catch (Exception e) {
			status=-2;
			Log.error("error:",e);
		}
		System.exit(status);
	}
	
	@Deprecated
	@SuppressWarnings("unused")
	private static String[] getFGP(String fileName){
		String [] groups=new String[]{};
		Pattern pattern = Pattern.compile(Configure.regx);
		Matcher m = pattern.matcher(fileName);
		if (m.matches()) {
			groups = new String[m.groupCount()];
			for (int i = 0; i < groups.length; i++) {
				groups[i] = m.group(i+1);
			}
		}
		return groups;
	}
	
	@SuppressWarnings("deprecation")
	private int run(long cols,String seq,String data_length,String primarykey_order_id,Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		Log.info("the cols is:========"+cols);
		Log.info("seq==="+seq);
		Log.info("data_length==="+data_length);
		Log.info("primarykey_id==="+primarykey_order_id);
		conf.set(Constants.COLNUMSIZE, String.valueOf(cols));
		conf.set(Constants.RECORDDELIMITER, Constants.CF);
		conf.set("mapred.job.queue.name", "bpsctb");
		conf.set("colSeq",seq);
		conf.set("Error_output", ErroroutputPath);
		conf.set("colData_length", data_length);
		conf.set("PrimaryFlag", primarykey_order_id);
		Job job = new Job(conf, JobUtil.getJobName(dataTime, interfaceCode));
		job.setJarByClass(DataQualityCheck.class);
		job.setMapperClass(DqcMapper_utf8.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextFormatForDQC.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		TextFormatForDQC.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		JobUtil.clean(conf,outputPath);
		JobUtil.clean(conf,ErroroutputPath);
		Log.info(outputPath);
		MultipleOutputs.addNamedOutput(job, Constants.OUTPUTFILENAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.ERRORFILENAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.ERRORDATAFILENAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.ERRORLOGFILENAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.PRIMARYKEYNULLNAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.DATEFORMATNAMEERROR,TextOutputFormat.class, Text.class, Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static void usage(final String errorMsg) {
	    if (errorMsg != null && errorMsg.length() > 0) {
	      System.err.println("ERROR: " + errorMsg);
	    }
	    System.err.println("Usage: SplitJob [options] <interface> <inputpath> <dataTime>");
	}
}
