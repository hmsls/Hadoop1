package com.xxx.hadoop1;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestCollWritable {
	@Test
	public void testArrayWritable() throws Exception, Exception{
		Writable[] data = new Text[10];
		for(int i = 1;i<=10;i++){
			data[i-1]=new Text("tom"+i);
		}
		ArrayWritable aw = new ArrayWritable(Text.class);
		aw.set(data);
		aw.write(new DataOutputStream(new FileOutputStream("d:/data.txt")));
	}
	@Test
	public void testMapWritable() throws Exception, Exception{
		MapWritable map = new MapWritable();
		for(int i = 0;i<10;i++){
			map.put(new IntWritable(i), new Text("tome"+i));
		}
		map.write(new DataOutputStream(new FileOutputStream("d:/map.txt")));
	}
}
