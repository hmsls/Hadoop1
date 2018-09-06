package MR;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Mapper
 */
public class MyMaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//
	private static final int MISSING = 9999;

	/**
	 * mapper
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		InetAddress addr = InetAddress.getLocalHost();
		//增加计数器，可以在客户端观察
		context.getCounter("group1", "mapper.setup"+addr).increment(1);;
	}
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//value String
		String line = value.toString();
		//提取年份
		String year = line.substring(15, 19);
		//气温
		int airTemperature;
		//提取气温值+ / -
		if (line.charAt(87) == '+') { 
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		
		//质量
		String quality = line.substring(92, 93);
		
		//判断气温的有效性
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		InetAddress addr = InetAddress.getLocalHost();
		System.out.println(System.currentTimeMillis()+": MyMaxTempMapper.cleanup : "+ addr + ":" + this.hashCode());
	}
	
}
