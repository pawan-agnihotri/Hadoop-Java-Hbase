package com.pooja.hbase;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CustomerFileReducer extends Reducer<Text, Text, Text, Text> {

	  @Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	 
		  context.write(key, values.iterator().next());
	  }
	}