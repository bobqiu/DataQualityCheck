/*
 * Name  :DBUtil.java
 * Author:guohao@teamsun.com
 * Date  : 2012-2-4
 * Copyright 2011 Teamsun, Inc. All rights reserved.
 */
package com.teamsun.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * @name DBUtil.java
 * @author guohao@teamsun.com.cn
 * @version Aml 2012-2-4
 */
public class DBUtil {
	private static final Logger logger = LoggerFactory.getLogger(DBUtil.class);
	static String driver;

	static String username;

	static String password;

	static String url;
    private static BasicDataSource ds = null;
	static {
		driver = Configure.database_driver;
		username = Configure.database_username;
		password = Configure.database_password;
		url = Configure.dburl;
		ds = new BasicDataSource();
		ds.setDefaultAutoCommit(false);
		ds.setTestOnBorrow(true);
		ds.setTestOnReturn(true);
		ds.setTestWhileIdle(true);
	    ds.setDriverClassName(driver);
	    ds.setUrl(url);
	    ds.setUsername(username);
	    ds.setPassword(password);
	    ds.setInitialSize(Configure.initialSize);
	    ds.setMinIdle(Configure.minIdle);
	    ds.setMaxActive(Configure.maxActive);
	    ds.setMaxIdle(Configure.maxIdle);
	    ds.setMaxWait(Configure.maxWait);
	    
	}
	public static void initDs(){
		logger.info("初始化数据源");
		Connection conn = getConn();
		try {
			conn.close();
		} catch (SQLException e) {
			logger.info("数据源初始化失败",e);
		}
		logger.info("数据源初始化完毕");
	}
	public static BasicDataSource getDs(){
		return ds;
	}
	public static Connection getConn() {
		Connection conn = null;
		try {
//			Class.forName(driver).newInstance();
//			conn = DriverManager.getConnection(url, username, password);
//			conn.setAutoCommit(false);
//			long t = System.currentTimeMillis();
			conn = ds.getConnection();
//			System.out.println(System.currentTimeMillis()-t);
			
		} catch (Exception e) {
			logger.info("获取数据库连接错误",e);
		}
		return conn;
	}

	public static String getSysDate() throws SQLException {
		return getSysDate(getConn());
	}

	public static String getSysDate(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("select to_char(sysdate,'yyyymmdd') from dual");
		ResultSet rs = ps.executeQuery();
		rs.next();
		String s = rs.getString(1);
		rs.close();
		ps.close();
		return s;
	}

	public static long queryForLong(String sql, Object[] param) throws SQLException {
		return queryForLong(sql, param, DBUtil.getConn());
	}

	public static long queryForLong(String sql, Object[] param, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		if (param != null && param.length >= 0) {
			for (int i = 0; i < param.length; i++) {
				ps.setObject(i + 1, param[i]);
			}
		}
		ResultSet rs = ps.executeQuery();
		rs.next();
		long l = rs.getLong(1);
		rs.close();
		ps.close();
		return l;
	}

	public static String queryForString(String sql, Object[] param)  {
		return queryForString(sql, param, DBUtil.getConn());
	}

	public static String queryForString(String sql, Object[] param, Connection conn)  {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String l = null;
		try {
			ps = conn.prepareStatement(sql);
			if (param != null && param.length >= 0) {
				for (int i = 0; i < param.length; i++) {
					ps.setObject(i + 1, param[i]);
				}
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				l = rs.getString(1);
			}
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				logger.error("数据库回滚失败：",e1);
			}
			logger.error("数据库查询失败：",e);
		}finally{
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				logger.error("释放数据库资源失败：",e);
			}
		}
		return l;
	}

	public static void update(String sql, Object[] param, Connection conn) throws SQLException {
		update(sql, param, conn, false);
	}

	public static int update(String sql, Object[] param) {
		return update(sql, param, getConn(), true);
	}

	public static void batchUpdate(String sql, List<Object[]> list, Connection conn)  {
		if (list == null || list.size() < 1)
			return;
		PreparedStatement ps=null;
		try {
			ps = conn.prepareStatement(sql);
			ps.clearBatch();
			for (int i = 0; i < list.size(); i++) {
				Object[] param = list.get(i);
				if (param == null)
					continue;
				for (int k = 0; k < param.length; k++) {
					ps.setObject(k + 1, param[k]);
				}
				ps.addBatch();
			}
		  ps.executeBatch();
		  conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {logger.error("数据库回滚失败",e1);}
			logger.error("数据库提交失败",e);
		}finally{
					try {
						if (ps!=null) {
						ps.close();
					}
						if (conn!=null) {
							conn.close();
						}
					} catch (SQLException e) {
						logger.error("释放数据库资源错误：",e);
					}
		}
	}

	public static Map<String, Object> queryForObject(String sql, Object[] param, Connection conn) throws SQLException {
		List<Map<String, Object>> l = _queryForObject(sql, param, conn, false);
		if (l == null || l.size() < 1)
			return null;
		return l.get(0);
	}

	public static Map<String, Object> queryForObject(String sql, Object[] param) {
		List<Map<String, Object>> l = _queryForObject(sql, param, getConn(), false);
		if (l == null || l.size() < 1)
			return null;
		return l.get(0);
	}

	public static List<Map<String, Object>> queryForObjectList(String sql, Object[] param)  {
		return _queryForObject(sql, param, getConn(), true);
	}

	public static List<Map<String, Object>> queryForObjectList(String sql, Object[] param, Connection conn)
			 {
		return _queryForObject(sql, param, conn, true);
	}

	public static String getPagingSql(String sql, int minNum, int maxNum) {
		 String sql0 =new StringBuffer("SELECT * FROM ( SELECT T.*, ROWNUM rn FROM (").append(sql)
				.append(") T WHERE ROWNUM <= ").append(maxNum).append(" ) TT  WHERE TT.rn > ").append(minNum)
				.toString();
		 return sql0;
	}

	private static List<Map<String, Object>> _queryForObject(String sql, Object[] param, Connection conn, boolean isList)
			 {
		PreparedStatement ps=null;
		ResultSet rs=null;
		List<Map<String, Object>> obj=null;
		try {
			ps = conn.prepareStatement(sql);
		if (param != null && param.length >= 0) {
			for (int i = 0; i < param.length; i++) {
				ps.setObject(i + 1, param[i]);
			}
		}
		 rs = ps.executeQuery();
		 obj = new ArrayList<Map<String, Object>>();
		ResultSetMetaData rsm = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> m = new HashMap<String, Object>();
			for (int i = 0; i < rsm.getColumnCount(); i++) {
				m.put(rsm.getColumnName(i + 1).toUpperCase(), rs.getObject(i + 1));
			}
			obj.add(m);
			if (!isList)
				break;
		}
		conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				logger.error("数据库回滚失败",e1);
			}
			logger.error("数据库提交失败",e);
		}finally{
			try {
				if(rs!=null)rs.close();
				if(ps!=null)ps.close();
				if(conn!=null)conn.close();
			} catch (SQLException e) { logger.error("释放数据库资源失败",e);}
		}
		return obj;
	}
	
	public static int update(String sql) throws SQLException{
		Connection conn = getConn();
		boolean commit = true;
		PreparedStatement ps = null;
		int count = 0;
		try {
			ps = conn.prepareStatement(sql);
			count = ps.executeUpdate();
			ps.close();
			conn.commit();
		}  catch (SQLException e) {
			if (commit) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.error("数据库回滚失败",e1);
				}
			}
			logger.error("数据库提交错误",e);
			throw new SQLException(e);
		}finally{
			if (commit) {
				try {
					if (ps!=null) {
						ps.close();
					}
					if (conn!=null) {
						conn.close();
					}
				} catch (SQLException e) {
					logger.error("释放数据库资源错误 ",e);
				}
			}
		}
		return count;
	}

	private static int update(String sql, Object[] param, Connection conn, boolean commit)  {
		PreparedStatement ps = null;
		int count = 0;
		try {
			ps = conn.prepareStatement(sql);
			if (param != null && param.length >= 0) {
				for (int i = 0; i < param.length; i++) {
					if (param[i]!=null) {
						ps.setObject(i + 1, param[i]);
					}else {
						ps.setObject(i+1, param[i], Types.NULL);
					}
				}
			}
			count = ps.executeUpdate();
			ps.close();
			if (commit) {
				conn.commit();
			}
		} catch (SQLException e) {
			if (commit) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.error("数据库回滚失败",e1);
				}
			}
			logger.error("数据库提交错误",e);
		}finally{
			if (commit) {
				try {
					if (ps!=null) {
						ps.close();
					}
					if (conn!=null) {
						conn.close();
					}
				} catch (SQLException e) {
					logger.error("释放数据库资源错误 ",e);
				}
			}
		}
		return count;
	}

}
