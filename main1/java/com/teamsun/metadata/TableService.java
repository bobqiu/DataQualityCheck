/**
 * 
 */
package com.teamsun.metadata;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
import com.teamsun.common.Constants;
import com.teamsun.common.DBUtil;
import com.teamsun.common.FileUtil;

/**
 * @author hetao
 *
 */
public class TableService {

	private static final Logger Log = LoggerFactory.getLogger(TableService.class);
	
	/**
	 * 查询表最新定义
	 * @param interfaceCode
	 * @return
	 */
	public List<Map<String,Object>> getTableDef(String interfaceCode){
		String sql = "select interface_code,field_code,order_id,field_type,is_pk,field_name from bd_int_fields_config where interface_code='"+interfaceCode+"' and end_date='"+Constants.MAX_DATE+"' order by order_id";
//		Log.info("sql="+sql);
		List<Map<String,Object>> lst = DBUtil.queryForObjectList(sql, null);
		return lst;
	}
	
	/**
	 * 根据接口统计字段个数
	 * @param interfaceCode
	 * @return
	 */
	public long countTableCols(String interfaceCode,String sysCode){
		String sql = "select count(*) as cnt from bd_int_fields_config where interface_code='"+interfaceCode+"'and sys_code='"+sysCode+"' and end_date='"+Constants.MAX_DATE+"'";
		try {
			return DBUtil.queryForLong(sql, null);
		} catch (SQLException e) {
			Log.error("",e);
			return -1;
		}
	}
	
	public String getColSeq(Configuration conf,String interfaceCode,String sysCode,String inPath) throws IOException{
		String[] cols = getTableCols(interfaceCode,sysCode);//配置库中的顺序
		String[] _cols = getTabCol(conf,inPath);//文件中的顺序
		HashMap<String,String> map = new HashMap<String,String>();
		for(int i=0;i<_cols.length;i++){
			map.put(_cols[i].toUpperCase(), String.valueOf(i));
		}
		String seq="";
		for(String str:cols){
			seq+=Constants.COMMA+map.get(str);
		}
		return seq.substring(1);
	}
	/**
	 * 根据接口统计字段长度及校验方法
	 */
	public String getColData_length(String interfaceCode,String sysCode) throws IOException{
		String[] cols = getTableCols_datalength(interfaceCode,sysCode);//配置库中的顺序
		String seq="";
		for(String str:cols){
			seq+=Constants.COMMA_1+str;
		}
		return seq.substring(1);
	}
	
	
	/**
	 * 根据接口统计主键的order_id
	 */
	public String getColPrimary_order_id(String interfaceCode,String sysCode) throws IOException{
		String[] cols = getTablePrimary(interfaceCode,sysCode);//配置库中的顺序
		String seq="";
		//如果该接口没有主键
		if("".equals(cols[0]) && cols.length == 1){
			return "0";
		}
		for(String str:cols){
			seq+=Constants.COMMA+str;
		}
		return seq.substring(1);
	}
	/**
	 * 从接口定义表中查询是主键的order_id
	 * @param interfaceCode
	 * @return
	 */
	private static String[] getTablePrimary(String interfaceCode,String sysCode){
		String[] tabColNames = null;
		String[] primaryflag = {""};
		String sql = "select order_id from bd_int_fields_config where interface_code='"+interfaceCode+"' and sys_code='"+sysCode+"' and end_date='"+Constants.MAX_DATE+"' and is_pk=0 order by order_id";
		Log.info("sql="+sql);
		List<Map<String, Object>> lst = DBUtil.queryForObjectList(sql, null);
		Log.info("list size:"+lst.size());
		if(lst.size() == 0){
			return primaryflag;
		}
		if(!lst.isEmpty() && lst.size()>0){
			tabColNames = new String[lst.size()];
			int i=0;
			for(Map<String, Object> map :lst){
				tabColNames[i]=map.get("ORDER_ID").toString();
				Log.info("tabColNames:"+tabColNames[i]);
				i++;
			}
		}
		return tabColNames;
	}
	
	/**
	 * 从接口定义表中查询表结构
	 * @param interfaceCode
	 * @return
	 */
	private static String[] getTableCols(String interfaceCode,String sysCode){
		String[] tabColNames = null;
		String sql = "select field_code from bd_int_fields_config where interface_code='"+interfaceCode+"'and sys_code='"+sysCode+"' and end_date='"+Constants.MAX_DATE+"' order by order_id";
		Log.info("sql="+sql);
		List<Map<String, Object>> lst = DBUtil.queryForObjectList(sql, null);
		Log.info("list size:"+lst.size());
		if(!lst.isEmpty() && lst.size()>0){
			tabColNames = new String[lst.size()];
			int i=0;
			for(Map<String, Object> map :lst){
				tabColNames[i]=(String) map.get("FIELD_CODE");
				Log.info("tabColNames:"+tabColNames[i]);
				i++;
			}
		}
		return tabColNames;
	}
	/**
	 * 从接口定义表中查询字段长度及校验规则
	 * @param interfaceCode
	 * @return
	 */
	private  String[] getTableCols_datalength(String interfaceCode,String sysCode){
		String[] tabColNames = null;
		String sql = "select FIELD_LENGTH||'+'||METHOD_CODE FM from bd_int_fields_config where interface_code='"+interfaceCode+"'and sys_code='"+sysCode+"' and end_date='"+Constants.MAX_DATE+"' order by order_id";
		Log.info("sql="+sql);
		List<Map<String, Object>> lst = DBUtil.queryForObjectList(sql, null);
		Log.info("list size:"+lst.size());
		if(!lst.isEmpty() && lst.size()>0){
			tabColNames = new String[lst.size()];
			int i=0;
			for(Map<String, Object> map :lst){
				tabColNames[i]=map.get("FM")==null ? "" : map.get("FM").toString();
				Log.info("tabColNames:"+tabColNames[i]);
				i++;
			}
		}
		return tabColNames;  
	}
	

	
	public String getSysCode(String interfaceCode){
		String sql = "select sys_code from bd_int_config where interface_code='"+interfaceCode+"'";
		return DBUtil.queryForString(sql, null);
	}
	
	
	  public void insert_pm_is_null(String sys_code,String interface_code,int sum_lines,String data_date) throws MySQLIntegrityConstraintViolationException, SQLException{
		  String sql = "insert into primarykey_is_null values('"+sys_code+"','"+interface_code+"',"+sum_lines+",'"+data_date+"')";
		   DBUtil.update(sql);
	   } 
	  public  void delete_pm_is_null(String sys_code,String interface_code,String data_date) throws MySQLIntegrityConstraintViolationException, SQLException{
		  String sql = "delete  primarykey_is_null where interface_code='"+interface_code+"'and sys_code='"+sys_code+"' and data_date='"+data_date+"'";
		   DBUtil.update(sql);
	   } 
	/**
	 * 比较接口文件中表结构和数据字典中表结构是否一致
	 * @param interfaceCode
	 * @param inPath
	 * @param conf
	 * @return
	 */
	public boolean checkTabCol(String interfaceCode,String sysCode,String inPath,Configuration conf) {
		Log.info("interfaceCode:"+interfaceCode+" inPath="+inPath);
		boolean flg=false;
		String[] cols = getTableCols(interfaceCode,sysCode);
		try {
			String[] _cols = getTabCol(conf,inPath);
			for(String a :_cols){
				Log.info("配置文件中的字段："+a);
			}
			if(compareStringArray(cols,_cols)){//字段完全一致
				flg=true;
			}else if(compareStringArray(sort(cols),sort(_cols))){//字段顺序发生变化
				flg=true;
			}
			return flg;
		} catch (IOException e) {
			Log.error("error:",e);
			return flg;
		}
	}
	
	/**
	 * 从接口文件中获取表结构
	 * @param inPath
	 * @return
	 * @throws IOException
	 */
	private String[] getTabCol(Configuration conf,String inPath) throws IOException {
		Log.info(inPath);
		List<String> lst = FileUtil.readLine(conf, new Path(inPath));
		if(null == lst || lst.isEmpty() || lst.size()==0)
			return new String[0];
		return lst.get(0).split(Constants.RF);
	}
	
	/**
	 * 对数组进行排序
	 * @param strArray
	 * @return
	 */
	private String[] sort(String[] strArray) {
		String _tmp="";
		for(String s : strArray){
			_tmp = _tmp+Constants.COMMA+s.toUpperCase();
		}
		String[] FirStr = _tmp.substring(1).split(Constants.COMMA);
		List<String> lst = Arrays.asList(FirStr);
		Collections.sort(lst);
		String[] retStr = new String[lst.size()];
		int i=0;
		for(String str:lst){
			retStr[i]=str;
			i++;
		}
		return retStr;
	}
	
	/**
	 * 比较两个数组内容是否一致
	 * @param str1
	 * @param str2
	 * @return
	 */
	private boolean compareStringArray(String[] str1,String[] str2) {
		String _tmp1="";
		String _tmp2="";
		if(str1.length != str2.length)
			return false;
		for(String _str1 :str1){
			_tmp1=_tmp1+Constants.COMMA+_str1;
		}
		for(String _str2 :str2){
			_tmp2=_tmp2+Constants.COMMA+_str2;
		}
		if(_tmp1.equals(_tmp2)){
			return true;
		}else{
			return false;
		}
	}

}
