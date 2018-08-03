/**
 * 
 */
package com.teamsun.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author hetao
 *
 */
public class FileNameBean {

	public FileNameBean(String fileName){
		String[] grp = splitFileName(fileName);
		this.sysCode=grp[0];
		this.interfaceCode=grp[1];
		this.dataTime=grp[3];
	}
	
	private String interfaceCode;
	private String sysCode;
//	private String tableName;
	private String dataTime;
	
	private String[] splitFileName(String fileName){
		String [] groups=new String[]{};
		Pattern pattern = Pattern.compile(Configure.regx);
		Matcher m = pattern.matcher(fileName);
		if (m.matches()) {
			groups = new String[m.groupCount()];
			for (int i = 0; i < groups.length; i++) {
				groups[i] = m.group(i+1);
			}
		}
		return groups;
	}
}
