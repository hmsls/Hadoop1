package MR_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MyWordCountApp {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("usage: inputPath outputPath");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		job.setJarByClass(MyWordCountApp.class);
		
		job.setJobName("word count");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		FileInputFormat.setMaxInputSplitSize(job, 15);
//		FileInputFormat.setMinInputSplitSize(job, 10);
		
		job.setNumReduceTasks(4);   //设置reduce的任务数
		
		job.setMapperClass(MyWordCountMapper.class);
		job.setReducerClass(MyWordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setCombinerClass(MyWordCountReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		
		System.exit(job.waitForCompletion(true)? 0:1);
	}
}
