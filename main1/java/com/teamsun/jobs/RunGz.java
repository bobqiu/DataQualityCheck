/**
 * 
 */
package com.teamsun.jobs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.common.Configure;
import com.teamsun.common.JobUtil;
import com.teamsun.mapred.DqcMapper_Gz;
import com.teamsun.reduce.DqcReduce;
/**
 * @author wpf
 *2016.10.17
 *处理逻辑集中，先进行压缩
 */
public class RunGz {
	
	private static final Logger Log = LoggerFactory.getLogger(RunGz.class);
	private static String interfaceCode; 
	private static String inputPath;
	private static String outputPath;
	private static String dataTime;
	

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
//		if(args.length>3 || args.length<2){
//			usage("Wrong number of arguments: " + args.length);
//			System.exit(-1);
//		}
		inputPath = args[0];
//		outputPath = args[1];
		interfaceCode=inputPath.split("/")[inputPath.split("/").length-1];
		dataTime=inputPath.split("/")[inputPath.split("/").length-2];
		Log.info(String.format("dataTime [%s],interface [%s],input [%s]", dataTime,interfaceCode,inputPath));
		
		int status = -1;
		try {
			Configure.init();
//			Configuration conf = new Configuration();
			status = new RunGz().run();
		} catch (Exception e) {
			status=-2;
			Log.error("error:",e);
		}
		System.exit(status);
	}
	
	
	@SuppressWarnings("deprecation")
	private  int run() throws IOException, ClassNotFoundException, InterruptedException{

		 Configuration conf = HBaseConfiguration.create();

		    conf.addResource("hbase-site.xml");
		    conf.set(TableOutputFormat.OUTPUT_TABLE, "tb_instid_org_rela");		  
		    UserGroupInformation.setConfiguration(conf);
		Job job = new Job(conf, JobUtil.getJobName(dataTime, interfaceCode));
		job.setJarByClass(RunGz.class);
		job.setMapperClass(DqcMapper_Gz.class);
		job.setReducerClass(DqcReduce.class);
//		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
//		job.addFileToClassPath(new Path("/SPLIT/jdbc/hbase.keytab"));
		//让job可以获取到相关认证的信息
//		User.getCurrent().obtainAuthTokenForJob(conf,job);
//		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
//		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
//		FileOutputFormat.setCompressOutput(job, true);
//		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		JobUtil.clean(conf_hbase,outputPath);
//		Log.info(outputPath);
		 return job.waitForCompletion(true) ? 0 : 1;
	}
	
	@SuppressWarnings("unused")
	private static void usage(final String errorMsg) {
	    if (errorMsg != null && errorMsg.length() > 0) {
	      System.err.println("ERROR: " + errorMsg);
	    }
	    System.err.println("Usage: SplitJob [options] <interface> <inputpath> <dataTime>");
	}
}
