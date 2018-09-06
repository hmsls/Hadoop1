package com.xxx.hadoop1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 映射文件
 * @author LISHUAI
 *
 */
public class TestMapFile {
	@Test
	public void testWrite() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("file:///d:/mymap.map");
		MapFile.Writer writer = new Writer(conf,fs,"file:///d:/mymap.map",IntWritable.class,Text.class);
//		IntWritable key = new IntWritable(1);
//		Text val = new Text("tom");
		writer.append(new IntWritable(1), new Text("tom1"));
		writer.append(new IntWritable(2), new Text("tom2"));
		writer.append(new IntWritable(3), new Text("tom3"));
		writer.append(new IntWritable(4), new Text("tom4"));
		writer.close();
		System.out.println("over");
	}
	@Test
	public void testReader() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("file:///d:/mymap.map");
		MapFile.Reader reader = new Reader(fs,"file:///d:/mymap.map",conf);
		IntWritable key = new IntWritable();
		Text val = new Text();
		while(reader.next(key, val)){
			System.out.println(key + ":" + val);
		}
		reader.get(new IntWritable(3), val);
		reader.close();
		System.out.println("over");
	}
	@Test
	public void write2() throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
//		conf.set("io.map.index.interval", "20");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("file:///d:/mymap.map");
		MapFile.Writer writer = new Writer(conf,fs,"file:///d:/mymap.map",IntWritable.class,Text.class);
		int indexInterval = writer.getIndexInterval();
		System.out.println(indexInterval);
//		设置索引间隔
//		writer.setIndexInterval(10);
		for(int i = 1;i<200;i++){
			writer.append(new IntWritable(i), new Text("tom"+i));
		}
		writer.close();
	}
	@Test
	public void testSearchClosest() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("file:///d:/mymap.map");
		MapFile.Reader reader = new Reader(fs,"file:///d:/mymap.map",conf);
		IntWritable key = (IntWritable) reader.getClosest(new IntWritable(9), new Text(),true);
		System.out.println(key);
		reader.close();
	}
	@Test
	public void preSeqFile() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		FileSystem fs = FileSystem.get(conf);
		MapFile.Writer writer = new Writer(conf,fs,"file:///d:/mySeq.seq",IntWritable.class,Text.class);
		for(int i = 1;i<=100;i++){
			writer.append(new IntWritable(i), new Text("tom"+i));
		}
		writer.close();
	}
	
}
