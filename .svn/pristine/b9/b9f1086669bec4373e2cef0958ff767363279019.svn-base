/**
 * 
 */
package com.teamsun.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author hetao
 * 
 */
public class JobUtil {

	public static String getJobName(String dataTime, String interfaceCode) {
		return dataTime + "-" + interfaceCode;
	}

	public static void clean(Configuration conf, String outPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path _outPath = new Path(outPath);
		if (fs.exists(_outPath)) {
			fs.delete(_outPath, true);
		}
		fs.close();
	}
}
