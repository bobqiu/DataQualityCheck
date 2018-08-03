/**
 * Copyright By 2016 TeamSun Co. Ltd. 
 * All right reserved
 */
package com.teamsun.common;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * datadqc
 * 
 * @author hetao
 * @since Apr 11, 2016
 * @version 1.0
 */
public class HdfsCodec {
	private static final Logger log = LoggerFactory.getLogger(HdfsCodec.class);

	public static void uncompress(String fileName,String outFileName) throws Exception {
		Class<?> codecClass = Class
				.forName("org.apache.hadoop.io.compress.GzipCodec");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		FSDataInputStream inputStream = fs.open(new Path(fileName));
		InputStream in = codec.createInputStream(inputStream);
		OutputStream out = fs.create(new Path(outFileName));
		IOUtils.copyBytes(in, out, conf);
		IOUtils.closeStream(in);
	}
	
	public static void main(String[] args) {
		String gzFileName= args[0];
		String outFileName=args[1];
		try {
			HdfsCodec.uncompress(gzFileName,outFileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
