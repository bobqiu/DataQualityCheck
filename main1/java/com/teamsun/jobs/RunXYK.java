/**
 * 
 */
package com.teamsun.jobs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
import com.teamsun.mapred.DqcMapper_RunXYK;
import com.teamsun.metadata.TableService;
/**
 * @author hetao
 *
 */
public class RunXYK {
	
	private static final Logger Log = LoggerFactory.getLogger(RunXYK.class);
	private static String interfaceCode; 
	private static String inputPath;
	private static String outputPath;
	private static String dataTime;
	private static String sysCode;
	private static String ErroroutputPath;
	

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
		if(args.length!=2){
			usage("Wrong number of arguments: " + args.length);
			System.exit(-1);
		}

		inputPath = args[0];
		outputPath = args[1];
	
		interfaceCode=inputPath.split("/")[inputPath.split("/").length-1];
		dataTime=inputPath.split("/")[inputPath.split("/").length-2];
		sysCode = inputPath.split("/")[inputPath.split("/").length-3];
		ErroroutputPath = Constants.ERROR_BASE_OUTPUT_PATH+sysCode+"/"+dataTime+"/"+interfaceCode+"/";
		Log.info(String.format("dataTime [%s],interface [%s],input [%s]", dataTime,interfaceCode,inputPath));
		int status = -1;
		try {
			Configure.init();
			DBUtil.initDs();
			Configuration conf = new Configuration();
			TableService ts = new TableService();
			long col_Size = ts.countTableCols(interfaceCode, sysCode);
			if(col_Size == 0){
				Log.error("TableStruct not exist...");
				status=-1;
				System.exit(status);
			}
			String data_length = ts.getColData_length(interfaceCode, sysCode);
			//获取是主键的order_id
			String primarykey_order_id = ts.getColPrimary_order_id(interfaceCode, sysCode);
			status = new RunXYK().run(col_Size,data_length,primarykey_order_id,conf);
			//如果map执行完成后返回值为0，判断目标文件中是否有error文件，有了以非0退出，作业失败
			if(status == 0){
				if(FileUtil.getErrorFileNameExists(conf,new Path(ErroroutputPath))){
					if(!FileUtil.getPrimaryNullFileName(conf,new Path(ErroroutputPath))){
						//若keynull文件存在，则更新数据库表primarykey_is_null表
						int sum_line = 0;
						sum_line = FileUtil.readFile2List(conf, ErroroutputPath);
						
						ts.delete_pm_is_null(sysCode, interfaceCode, dataTime);
						ts.insert_pm_is_null(sysCode, interfaceCode, sum_line, dataTime);
						Log.info("primarykey_is_null success insert into "+sum_line+ " rows!!");
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

	
	@SuppressWarnings("deprecation")
	private int run(long cols,String data_length,String primarykey_order_id,Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		Log.info("the cols is:========"+cols);
		Log.info("data_length==="+data_length);
		Log.info("primarykey_id==="+primarykey_order_id);
		conf.set(Constants.RECORDDELIMITER, "|\n");
		conf.set("mapred.job.queue.name", "bpsctb");
		conf.set("Error_output", ErroroutputPath);
		conf.set(Constants.COLNUMSIZE, String.valueOf(cols));
		conf.set("colData_length", data_length);
		conf.set("PrimaryFlag", primarykey_order_id);
		Job job = new Job(conf, JobUtil.getJobName(dataTime, interfaceCode));
		job.setJarByClass(RunXYK.class);
		job.setMapperClass(DqcMapper_RunXYK.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
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
