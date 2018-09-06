package com.xxx.hadoop1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Persion implements Writable{
	private int id;
	private String name;
	private int age;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
		out.write(age);
	}
	public void readFields(DataInput in) throws IOException{
		this.id = in.readInt();
		this.name = in.readUTF();
		this.age = in.readInt();
	}
	
}
