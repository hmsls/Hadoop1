package MR.Sort;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * reduce阶段
 */
public class MyMaxTempReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//
		InetAddress addr = InetAddress.getLocalHost();
		System.out.println("reducer : " + addr.getHostAddress());
		int maxValue = Integer.MIN_VALUE;
		int count = 0;
		// 提取年度的最高气温
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
			count++;
		}
		context.getCounter("r", "MaxReduce."+key.toString()).increment(count);

		// 写入输出
		context.write(key, new IntWritable(maxValue));
	}

}
