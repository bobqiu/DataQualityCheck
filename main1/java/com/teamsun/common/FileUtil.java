/**
 * 
 */
package com.teamsun.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teamsun.exception.TitleNotFoundException;

/**
 * datadqc
 * @author hetao
 * @since  Nov 6, 2015 
 * @version 1.0 
 */
public class FileUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

	/**
	 * 本地路径按行读取文件内容
	 * @param filePath
	 * @param encoding
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLine2List(String filePath, String encoding)
			throws IOException {
		File file = new File(filePath);
		if (file.isDirectory())
			throw new IOException("Parameter 'file' is a directory");
		return FileUtils.readLines(file, encoding);
	}
	

	/**
	 * hdfs上按行读取文件内容
	 * @param conf
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static List<String> readLine(Configuration conf,Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		List<String> lst = new ArrayList<String>();
		try{
			if(fs.isFile(path)){
				FSDataInputStream fis = fs.open(path);
				//考虑添加encoding
				String encoding="UTF-8";
				BufferedReader in = null;
				String line;
				try {
					in = new BufferedReader(new InputStreamReader(fis,encoding));
					while ((line = in.readLine()) != null) {
						lst.add(line);
					}
				} catch (IOException e) {
					log.error("", e);
				} finally {
					if(null != in)
						in.close();
				}
			}
		}catch(Throwable ex){
			ex.printStackTrace();
			log.error("error:",ex);
		}
		return lst;
	}
	
	
	/**
	 * hdfs上按行读取文件行数
	 * @param conf
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static int readLine_count(Configuration conf,Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		long lst = 0;
		try{
			if(fs.isFile(path)){
				FSDataInputStream fis = fs.open(path);
				BufferedReader in = null;
				String line;
				try {
					in = new BufferedReader(new InputStreamReader(fis));
					while ((line = in.readLine()) != null) {
						lst++;
					}
				} catch (IOException e) {
					log.error("", e);
				} finally {
					if(null != in)
						in.close();
				}
			}
		}catch(Throwable ex){
			ex.printStackTrace();
			log.error("error:",ex);
		}
		return (int) lst;
	}
	
	/**
	 * 循环读取文件夹下所有文件内容
	 * @param conf
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static int readFile2List(Configuration conf,String filePath) throws IOException{
		int lst = 0 ;
		FileSystem fs = FileSystem.get(conf);
		fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(new Path(filePath));
		for(int i=0;i<fileStatus.length;i++){
            if(fileStatus[i].isFile() && fileStatus[i].getPath().getName().startsWith(Constants.TAB_STRUCTURE_FILE_SUFFIX_keynull)){
                Path p = new Path(fileStatus[i].getPath().toString());
//                lst.addAll(readLine(conf,p));
                lst += readLine_count(conf,p);
            }
        }
		return lst;
	}
	
	public static boolean delFile(Configuration conf,Path path,String titlename) throws IOException{
		boolean b =false;
		try{
			FileSystem fs = FileSystem.get(conf);
			fs.delete(path);
			b=true;
		}catch (Throwable e){
			e.printStackTrace();
			log.error("error:",e);
			throw new IOException(e);
		}
		 return b;
	}
	
	/**
	 * 
	 * @param conf
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static String getTitleFileName(Configuration conf,Path path) throws TitleNotFoundException{
		try {
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(path);
			for(FileStatus file: files){
				if(file.getPath().getName().endsWith(Constants.TAB_STRUCTURE_FILE_SUFFIX))
					return file.getPath().getName();
			}
			throw new TitleNotFoundException("title file not find...");
		} catch (IOException e) {
			log.error("eorror:",e);
			throw new TitleNotFoundException("title file not find...");
		}
	}
	
	//查看是否有error文件存在
	public static boolean getErrorFileName(Configuration conf,Path path) throws IOException{

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(path);
			for(FileStatus file: files){
				if(file.getPath().getName().startsWith(Constants.TAB_STRUCTURE_FILE_SUFFIX_ERROR)){
					return false;
				}
			}
			return true;

	}
	//查看是否有Primarykeynull文件存在
	public static boolean getPrimaryNullFileName(Configuration conf,Path path) throws IOException{

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(path);
			for(FileStatus file: files){
				if(file.getPath().getName().startsWith(Constants.TAB_STRUCTURE_FILE_SUFFIX_keynull)){
					return false;
				}

			
			
			
			}
			return true;

	}
	//判断目标目录是否存在
	public static boolean getErrorFileNameExists(Configuration conf,Path path) throws IOException{

			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(path)){
				return true;
			}else {
				return false;
			}
			

	}
	public static boolean writeTitle(String[] lst,Configuration conf,Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		OutputStream out = fs.create(path);
		StringBuffer line = new StringBuffer();
		int i=0;
		for(String str:lst){
			if(i == 0){
				line.append(str);
			}else{
				line.append("|+|"+str);
			}
			i++;
		}
		out.write(line.toString().getBytes());
		out.flush();
		out.close();
		return false;
	}
	
	public static boolean writeMeta(List<Map<String,Object>> metaMapList,Configuration conf,Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		OutputStream out = fs.create(path);
		StringBuffer line = new StringBuffer();
//		String field_name = "";
		for(Map<String,Object> map:metaMapList){
			/*if(null!= map.get("FIELD_NAME")){
				field_name=map.get("FIELD_NAME").toString();
			}*/
			line.append("|"+(null==map.get("INTERFACE_CODE")?"":map.get("INTERFACE_CODE").toString()))
				.append("|+|"+(null==map.get("FIELD_CODE")?"":map.get("FIELD_CODE").toString()))
				.append("|+|"+(null==map.get("ORDER_ID")?"":map.get("ORDER_ID").toString()))
				.append("|+|"+(null==map.get("FIELD_TYPE")?"":map.get("FIELD_TYPE").toString()))
				.append("|+|"+(null==map.get("IS_PK")?"":map.get("IS_PK").toString()))
				.append("|+|"+(null==map.get("FIELD_NAME")?"":map.get("FIELD_NAME").toString()))
//				.append("|+|"+field_name)
				.append("|-|\n");
		}
		out.write(line.toString().getBytes());
		out.flush();
		out.close();
		return false;
	}
	
}
