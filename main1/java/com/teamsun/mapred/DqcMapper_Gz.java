/**
 * 
 */
package com.teamsun.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author wpf
 * 2016.10.17
 * 
 */
public class DqcMapper_Gz extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
////		String sv = new String(value.getBytes(),0,value.getLength(),"GBK");
//		IntWritable sv = new IntWritable(Integer.parseInt(value.toString()));
////		mos.write(Constants.OUTPUTFILENAME, sv, sv);
//		context.write(sv, sv);
//		String[] values = value.toString().split("|");
		String sv = value.toString();
		String key1 = sv.split("\\|")[1];
		String value1 = sv.split("\\|")[0];
//		context.getCounter("prov",sv).increment(1);
		context.write(new Text(key1), new Text(value1));	
	}	
}
