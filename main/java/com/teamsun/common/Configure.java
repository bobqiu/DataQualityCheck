/**
 * 
 */
package com.teamsun.common;

import java.util.Properties;


/**
 * @author hetao
 *
 */
public class Configure {
	public static String database_driver;
	public static String database_username;
	public static String database_password;
	public static String dburl;
	public static int initialSize;
	public static int maxActive;
	public static int maxIdle;
	public static int minIdle;
	public static int maxWait;
	
	public static String regx;
	
	public static String destdir;
	public static int CHKSTATTIME;
	public static int COREPOOLSIZE;
	public static int MAXIMUMPOOLSIZE;
	
	public static String label;
	public static String user;
	public static String password;
	public static String opencode;
	
	public static int recv_num=3;
	public static String MACHINENUM;
	
	/*public static String tmpdir;
	public static String bakdir;
	public static String recvbak;*/
	
	public static void init(){
		Properties p = PropertiesUtil.getProperties("datarecv.properties");
		database_driver = p.getProperty("database_driver");

		database_username = p.getProperty("database_username");
		database_password = p.getProperty("database_password");
		dburl = p.getProperty("dburl");
		initialSize = Integer.parseInt(p.getProperty("initialSize"));
		maxActive = Integer.parseInt(p.getProperty("maxActive"));
		maxIdle = Integer.parseInt(p.getProperty("maxIdle"));
		minIdle = Integer.parseInt(p.getProperty("minIdle"));
		maxWait = Integer.parseInt(p.getProperty("maxWait"));
		regx = p.getProperty("regx");
		destdir = p.getProperty("destdir");
		CHKSTATTIME = Integer.parseInt(p.getProperty("chkstatTime"));
		COREPOOLSIZE = Integer.parseInt(p.getProperty("corePoolSize"));
		MAXIMUMPOOLSIZE = Integer.parseInt(p.getProperty("maximumPoolSize"));
		label = p.getProperty("label");
		user = p.getProperty("user");
		password = p.getProperty("password");
		opencode = p.getProperty("opencode");
		MACHINENUM = p.getProperty("machine_num");
	}
}
