package com.teamsun.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


/**
 * @author wpf
 *
 */
public class DqcReduce extends TableReducer<Text, Text, NullWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {// 一对一关系	
		for (Text value : values) {
			val = value.toString();
			break;
		}
		
//		String sv = key.toString();
//		mos.write(Constants.OUTPUTFILENAME, null, new Text(sv));
		Put row = new Put(Bytes.toBytes(key.toString()));
		row.add(Bytes.toBytes("f"), Bytes.toBytes("org"), Bytes.toBytes(val));
	
		context.write(NullWritable.get(), row);
	}
	String val= "";

	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}
}
