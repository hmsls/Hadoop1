package com.xxx.hadoop1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestSerializableIntWritable {
	@Test
	public void testJVMSerializable() throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		IntWritable i = new IntWritable(355);
		i.write(oos);
		oos.close();
		baos.close();
		byte[] bys = baos.toByteArray();
		System.out.println(bys.length);
	}
	@Test
	public void testHaoopSerializable() throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		IntWritable i = new IntWritable(355);
		i.write(dos);
		dos.close();
		baos.close();
		byte[] bys = baos.toByteArray();
		System.out.println(bys.length);
		
		//反序列化
		IntWritable i2 = new IntWritable();
		i2.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
		System.out.println(i2.get());
	}
}
