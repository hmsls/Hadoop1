package com.xxx.hadoop1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;


/**
 * 写入sequence文件
 * SequenceFile  key-value，类似于map
 * @author LISHUAI
 *
 */
public class TestSequenceFile {
	@Test
	public void write() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		
		Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		IntWritable key = new IntWritable(1);
		Text value = new Text("tom");
		
		for(int i = 0;i<100;i++){
			key.set(i);
			value.set("tom"+i);
			writer.append(key, value);
		}
//		writer.append(new IntWritable(1), new Text("tom1"));
//		writer.append(new IntWritable(1), new Text("tom2"));
//		writer.append(new IntWritable(1), new Text("tom3"));
//		writer.append(new IntWritable(1), new Text("tom4"));
//		writer.close();
		System.out.println("over");
	}
	@Test
	public void writeWithSync() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		IntWritable key = new IntWritable();
		Text val = new Text();
		
		int count = 5;
		for(int i = 0;i<100;i++){
			key.set(i);
			val.set("li"+i);
			writer.append(key, val);
			if(i % count == 0){
				writer.sync();
			}
		}
	}
	@Test
	public void readSeq() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		while(reader.next(key, value)){
			System.out.println(key+"="+value);
		}
		reader.close();
	}
	@Test
	public void readSeqPosition() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		Class key1 = reader.getKeyClass();
		Class value1 = reader.getValueClass();
		CompressionCodec codec = reader.getCompressionCodec();
		CompressionType type = reader.getCompressionType();
		long pos = reader.getPosition();
		System.out.println(pos);
		//reader.getCurrentValue(key1);
		while(reader.next(key, value)){
			System.out.println(pos+":"+key+"="+value);
			pos = reader.getPosition();
		}
		reader.close();
	}
	@Test
	public void testSeek() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		reader.seek(253);
		reader.next(key,value);
		System.out.println(key+":"+value);
	}
	@Test
	public void sync() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile.sqe");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		int sycnPos = 0;
		reader.sync(sycnPos);
		long pos = reader.getPosition();
		reader.next(key, value);
		System.out.println(sycnPos+ ":"+pos+"-"+key+"-"+value);
		reader.close();
	}
	/**
	 * 使用压缩写入SequenceFile(SnappyCodec)
	 * @throws Exception 
	 */
	@Test
	public void writeInCompress() throws Exception{
		Configuration conf  = new Configuration();
		conf.set("fs.defaultFS",  "hdfs://192.168.174.128");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://192.168.174.128:8020/user/lishuai/data/mysqefile11.sqe");
		CompressionCodec codec = ReflectionUtils.newInstance(SnappyCodec.class, conf);
		Writer writer = SequenceFile.createWriter(conf, fs.create(path),IntWritable.class, Text.class, CompressionType.BLOCK, codec);
		IntWritable key = new IntWritable();
		Text val = new Text();
		for(int i = 0;i<100;i++){
			key.set(i);
			val.set("tom"+i);
			writer.append(key, val);
		}
		System.out.println("over");
		
	}
}
