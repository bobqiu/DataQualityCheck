/**
 * 
 */
package com.teamsun.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.teamsun.common.Constants;

/**
 * @author wpf
 * 
 */
public class DqcMapper_Run_Replace extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sv = value.toString();		
		mos.write(Constants.OUTPUTFILENAME, null, new Text(replaceRF(sv)));
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	public String replaceRF(String value){
		return value.replaceAll(Constants.RF_DUPLICATION, Constants.RF);
	}
	String val ;
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
	
}
