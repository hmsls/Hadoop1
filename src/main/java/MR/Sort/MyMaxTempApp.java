package MR.Sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReducer App类
 */
public class MyMaxTempApp {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		
//		Configuration conf  = job.getConfiguration();
		Configuration conf = job.getConfiguration();
		conf.set("fs.defaultFS", "file:///");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		//mapreduce.job.jar
		job.setJarByClass(MyMaxTempApp.class);
		
		//mapreduce.job.name
		job.setJobName("Max temp");								//设置作业名称
		
		//mapreduce.input.fileinputformat.inputdir
		FileInputFormat.addInputPath(job, new Path(args[0]));	//输入路径
		//mapreduce.output.fileoutputformat.outputdir
		
		//通过程序设置最小切片和最大切片
//		job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize", 15);
//		job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.minsize", 10);
		FileInputFormat.setMaxInputSplitSize(job, 15);
		FileInputFormat.setMinInputSplitSize(job, 10);
		
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	//输出路径
		job.setMapperClass(MyMaxTempMapper.class);				//设置Mapper类型
		job.setReducerClass(MyMaxTempReducer.class);			//设置reducer类型
		job.setOutputKeyClass(Text.class);						//设置输出key类型
		job.setOutputValueClass(IntWritable.class);				//设置输出value类型
		job.setCombinerClass(MyMaxTempReducer.class);   //设置Mapper端合成类，减少map到reduce的传输量
		
		
//		System.out.println(conf.get("fs.defaultFS"));
		
		//开始执行job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
