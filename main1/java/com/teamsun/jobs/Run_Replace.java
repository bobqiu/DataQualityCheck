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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.common.Configure;
import com.teamsun.common.Constants;
import com.teamsun.common.DBUtil;
import com.teamsun.common.JobUtil;
import com.teamsun.input.TextFormatForDQC;
import com.teamsun.mapred.DqcMapper_Run_Replace;
/**
 * @author wpf
 *解决源系统字段中存在|+|替换为|*|,进行还原
 */
public class Run_Replace {
	
	private static final Logger Log = LoggerFactory.getLogger(RunTxt.class);
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
		if(args.length != 2){
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
			status = new Run_Replace().run(conf);
		} catch (Exception e) {
			status=-2;
			Log.error("error:",e);
		}
		System.exit(status);
	}

	
	@SuppressWarnings("deprecation")
	private int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		conf.set("mapred.job.queue.name", "bpsctb");
		Job job = new Job(conf, JobUtil.getJobName(dataTime, interfaceCode));
		job.setJarByClass(RunTxt.class);
		job.setMapperClass(DqcMapper_Run_Replace.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextFormatForDQC.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		JobUtil.clean(conf,outputPath);
		JobUtil.clean(conf,ErroroutputPath);
		Log.info(outputPath);
		MultipleOutputs.addNamedOutput(job, Constants.OUTPUTFILENAME,TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constants.ERRORFILENAME,TextOutputFormat.class, Text.class, Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static void usage(final String errorMsg) {
	    if (errorMsg != null && errorMsg.length() > 0) {
	      System.err.println("ERROR: " + errorMsg);
	    }
	    System.err.println("Usage: SplitJob [options] <interface> <inputpath> <dataTime>");
	}
}
