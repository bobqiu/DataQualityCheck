package com.teamsun.jobs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseTest {
	 private static Configuration conf = null;
	static{
		Configuration conf_hbase = HBaseConfiguration.create();
		conf_hbase.addResource("hbase-site.xml");
		conf_hbase.addResource("yarn-site.xml");
	}
	public static void creatTable(String tableName, String[] familys) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for(int i=0; i<familys.length; i++){
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }
	public static void main(String[] args) {
		String tablename = "scores4";
        String[] familys = {"grade", "course"};
        try {
			HbaseTest.creatTable(tablename, familys);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
