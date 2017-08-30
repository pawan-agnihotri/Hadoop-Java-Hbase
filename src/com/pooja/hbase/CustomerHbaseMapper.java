package com.pooja.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.primitives.Bytes;

public class CustomerHbaseMapper extends TableMapper<Text, Text>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Text mapkey = new Text(key.get());
		Text mapvalue = new Text(value.getRow());
		context.write(mapkey, mapvalue);

	}
	

}
