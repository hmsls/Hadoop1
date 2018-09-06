package com.xxx.hadoop1;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class testTxt {
	@Test
	public void test1(){
		Text text = new Text("helloworld");
		int v = text.charAt(0);
		System.out.println((char)v);
	}
	@Test
	public void serialize() throws Exception{
		FileOutputStream fos = new FileOutputStream("d:/data.txt");
		DataOutputStream dos = new DataOutputStream(fos);
		IntWritable i = new IntWritable(100);
		i.write(dos);
		
		LongWritable l = new LongWritable(-10);
		l.write(dos);
		
		Text txt = new Text("lishuai");
		txt.write(dos);
		
		txt.set("haohaohao");
		txt.write(dos);
		
		i.set(-100);
		i.write(dos);
		
		dos.close();
		fos.close();
		System.out.println("over");
		
		FileInputStream fis = new FileInputStream("d:/data.txt");
		DataInputStream dis = new DataInputStream(fis);
		byte[] bys = new byte[fis.available()];
		fis.read(bys);
		fis.close();
		
		DataInputStream diss = new DataInputStream(new ByteArrayInputStream(bys));
		i.readFields(diss);
		System.out.println(i.get());
		
		l.readFields(diss);
		System.out.println(l.get());
		
		txt.readFields(diss);
		System.out.println(txt.toString());
		
		txt.readFields(diss);
		System.out.println(txt.toString());
		
		i.readFields(diss);
		System.out.println(i.get());
		
		
	}
}
