package com.teamsun.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_uniq {
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) 
	throws IOException, InterruptedException {
		String sv = key.toString();
//		mos.write(Constants.OUTPUTFILENAME, null, new Text(sv));
		context.write(null, new Text(sv));
	}
}
