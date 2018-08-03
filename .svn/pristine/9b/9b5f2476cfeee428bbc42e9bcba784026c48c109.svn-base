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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.common.Constants;

/**
 * @author wpf
 *
 */
public class DqcMapper_uniq extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sv = new String(value.getBytes(),0,value.getLength(),"GBK");
		context.write(new Text(sv), new Text(""));	
}
}

