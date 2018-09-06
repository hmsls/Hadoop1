package com.xxx.hadoop1;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestCompara {
	@Test
	public void testCompareTo(){
		IntWritable i1 = new IntWritable(1);
		IntWritable i2 = new IntWritable(2);
		System.out.println(i1.compareTo(i2));
	}
	/*
	 * 
	 */
	@Test
	public void testComparetor(){
		IntWritable.Comparator c = new IntWritable.Comparator();
		IntWritable i1 = new IntWritable(1);
		IntWritable i2 = new IntWritable(2);
		System.out.println(c.compare(i1, i2));
	}
}
