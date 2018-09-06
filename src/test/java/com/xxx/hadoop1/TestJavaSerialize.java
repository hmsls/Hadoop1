package com.xxx.hadoop1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

public class TestJavaSerialize {
	@Test
	public void serialize() throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(new AppTestSerialize("hashiqi"));
		oos.close();
		baos.close();
		System.out.println(baos.toByteArray().length);
	}
	@Test
	public void customer() throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		Persion p = new Persion();
		p.setAge(12);
		p.setName("nihao");
		p.setId(11);
		p.write(oos);
		oos.close();
		baos.close();
		System.out.println("over");
	} 
	@Test
	public void server() throws IOException{
		Persion p = new Persion();
		byte[] buf = new byte[1024];
		ByteArrayInputStream baos  = new ByteArrayInputStream(buf);
		ObjectInputStream oos = new ObjectInputStream(baos);
		p.readFields(oos);
		while(buf.length>0){
			int a = oos.read();
			System.out.println(a);
		}
	}
}
