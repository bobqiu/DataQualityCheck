/**
 * 
 */
package com.teamsun.common;

/**
 * @author hetao
 *
 */
public class Constants {
	
	public final static String ENCODING = "utf-8";
	public final static String RECORDDELIMITER="textinputformat.record.delimiter";
	public final static String CF="|-|\n";
	public final static String LF="|-|";
	public final static String LINUX_LF="\n";
	public final static String LINUX_LF_2="\r";
	public final static String LINUX_LF_3="\t";
	public final static String ORACLE_null="null";
	public final static String ORACLE_NULL="NULL";
	public final static String HIVE_RF="\u0001";
	public final static String HIVE_LF="\u0010";
	public final static String HIVE_="";
	public final static String RF_CH="|+|";
	public final static String RF="\\|\\+\\|";
	public final static String RF1="\\|";
	public final static String RF2="|";
	public final static String RF_DUPLICATION="\\|\\*\\|";
	public final static String REGX="(\\d{11})_(.*)_(\\d{4})_(\\d{8})_(\\w)_(\\d{4})_(\\d{4}).(.*)";
	
	public final static String COLNUMSIZE ="colnumSize";
	public final static String FIELDDELIMITER = "fieldDelimiter";
	
	public final static String TAB_STRUCTURE_FILE_SUFFIX=".title";
	public final static String TAB_STRUCTURE_FILE_SUFFIX_ERROR="error";
	public final static String TAB_STRUCTURE_FILE_SUFFIX_keynull="PrimaryKeyNull";
	public final static String TAB_STRUCTURE_FILE_SUFFIX_1=".xml";
	
	public final static String MAX_DATE="99991231";
	
	public final static String BASE_OUTPUT_PATH="/data/recv/";
	public final static String ERROR_BASE_OUTPUT_PATH="/data/ERROR/";
	public final static String BASE_INPUT_PATH="/data/tsrecv/";
	public final static String COMMA=",";
	public final static String COMMA_1="!";
	public final static String OUTPUTFILENAME="00000";
	public final static String ERRORFILENAME="errorColLength";
	public final static String ERRORDATAFILENAME="errorDataLength";
	public final static String ERRORLOGFILENAME="errorLog";
	public final static String PRIMARYKEYNULLNAME="PrimaryKeyNull";
	public final static String DATEFORMATNAMEERROR="DateFormatError";
}
