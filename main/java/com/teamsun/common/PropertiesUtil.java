/**
 * 
 */
package com.teamsun.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author hetao
 *
 */
public class PropertiesUtil {

	private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

	protected static Properties p =  new Properties();

	public static Properties getProperties(String propertyFileName) {
		
		 InputStream in = null;
		try {
			in = PropertiesUtil.class.getClassLoader().getResourceAsStream(propertyFileName);
			p.load(in);
		} catch (IOException e) {
			logger.error("load " + propertyFileName + " into Constants error!");
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					logger.error("close " + propertyFileName + " error!");
				}
			}
		}
		return p;
	}

	public static String getProperty(String key, String defaultValue) {
		return p.getProperty(key);
	}
	
	public static void main(String[] args) {
		String a = PropertiesUtil.getProperties("datarecv.properties").getProperty("dburl");
		System.out.println(a);
	}
}
