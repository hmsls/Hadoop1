package MR_wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyWordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		context.getCounter("r", "WCReducer.setUp").increment(1);
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text,IntWritable,Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable i : value){
			count++;
		}
		context.write(key, new IntWritable(count));
		context.getCounter("r", "WCReducer.reducer").increment(1);
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		context.getCounter("r", "WCReducer.cleanUp").increment(1);
	}
}
