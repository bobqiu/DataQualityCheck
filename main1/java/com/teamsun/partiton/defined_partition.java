package com.teamsun.partiton;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class defined_partition extends Partitioner<IntWritable, IntWritable>{

	@Override
	public int getPartition(IntWritable key,IntWritable value,int NumPartitions) {
		int keyInt = Integer.parseInt(key.toString());
		if(keyInt < 10000)
			return 0;
		else if(keyInt < 20000)
			return 1;
		else 
			return 2;		
	}
}
