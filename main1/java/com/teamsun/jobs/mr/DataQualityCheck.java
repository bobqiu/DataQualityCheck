package com.teamsun.jobs.mr;

/**
 * 
 */
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.common.Configure;
import com.teamsun.common.Constants;
import com.teamsun.common.DBUtil;
import com.teamsun.common.JobUtil;
import com.teamsun.mapred.DqcMapper_uniq;
import com.teamsun.reduce.DqcReduce;
/**
 * @author wpf
 *
 */
public class DataQualityCheck {
	
	private static final Logger Log = LoggerFactory.getLogger(DataQualityCheck.class);
	private static String interfaceCode; 
	private static String inputPath;
	private static String outputPath;
	private static String dataTime;
	private static String sysCode;
//	private static String ddlPath;
	
	public static void main1(String[] args) throws IllegalArgumentException, IOException {
//		Configuration conf = new Configuration();
//		FileUtil.getTitleFileName(conf, new Path("/data/recv/99340000000/20150319/LOAN-TB_LON_LOAN/"));
	}
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
//		interfaceCode = args[0];
		inputPath = args[0];
//		dataTime = args[2];
		outputPath = args[1];

//		sysCode = args[3];
		
		/*interfaceCode="LOAN-TB_LON_LOAN";
		sysCode="99340000000";
		dataTime="20150319";
		inputPath="/data/recv/99340000000/20150319/LOAN-TB_LON_LOAN/";*/
		interfaceCode=inputPath.split("/")[inputPath.split("/").length-1];
		dataTime=inputPath.split("/")[inputPath.split("/").length-2];
		sysCode = inputPath.split("/")[inputPath.split("/").length-3];
//		ErroroutputPath = Constants.ERROR_BASE_OUTPUT_PATH+sysCode+"/"+dataTime+"/"+interfaceCode+"/";
		Log.info(String.format("dataTime [%s],interface [%s],input [%s]", dataTime,interfaceCode,inputPath));
		int status = -1;
		try {
			Configure.init();
			DBUtil.initDs();
			Configuration conf = new Configuration();
			status = new DataQualityCheck().run(conf);
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
	private int run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		conf.set(Constants.RECORDDELIMITER, Constants.CF);
		conf.set("mapred.job.queue.name", "bpsctb");
		Job job = new Job(conf, JobUtil.getJobName(dataTime, interfaceCode));
		job.setJarByClass(DataQualityCheck.class);
		job.setMapperClass(DqcMapper_uniq.class);
		job.setReducerClass(DqcReduce.class);
//		job.setNumReduceTasks(200);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		JobUtil.clean(conf,outputPath);
		Log.info(outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static void usage(final String errorMsg) {
	    if (errorMsg != null && errorMsg.length() > 0) {
	      System.err.println("ERROR: " + errorMsg);
	    }
	    System.err.println("Usage: SplitJob [options] <interface> <inputpath> <dataTime>");
	}
}

