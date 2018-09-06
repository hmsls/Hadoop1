package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyApp {
	public static void main(String[] args) throws Exception {
		if(args.length != 3){
			System.err.println("useage: <inputPath> <outputPath> <customersDir>");
		}
		
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		
		job.setJarByClass(MyApp.class);
		job.setJobName("join app");
		
		conf.set("customersDir",args[2]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
