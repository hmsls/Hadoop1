package com.xxx.hadoop1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MyTestSequence {
	@Test
	public void testWrite() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/sequence1.sqe");
		Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		IntWritable key = new IntWritable();
		Text val = new Text();
		for(int i = 1;i<=100;i++){
			key.set(i);
			val.set("tom"+i);
			writer.append(key,val);
			if(i % 5 == 0){
				writer.sync();
			}
		}
		writer.close();
		System.out.println("over");
	}
	@Test
	public void testRead() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.174.128:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/sequence1.sqe");
		Reader reader = new SequenceFile.Reader(fs,path,conf);
		
		IntWritable key = new IntWritable();
		Text val = new Text();
		
		reader.sync(129);
		System.out.println(reader.getPosition());
		long pos=reader.getPosition();
		reader.next(key, val);
		reader.getCurrentValue(val);
		System.out.println(pos+":"+key+":"+val);
//		while(reader.next(key, val)){
//			reader.next(key, val);
//			System.out.println(key+":"+val);
//		}
	}
}
