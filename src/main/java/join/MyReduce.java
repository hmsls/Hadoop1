package join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<IntWritable, Text, Text, NullWritable> {

	private Map<Integer,String> map = null;
	@Override
	protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream fis = fs.open(new Path(conf.get("customersDir")));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		map = new HashMap<Integer,String>();
		while((line = reader.readLine())!= null){
			map.put(new Integer(line.split("\t")[0]), line);
		}
		reader.close();
		IOUtils.closeStream(fis);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int cid = key.get();
		for(Text order : values){
			String info = map.get(cid);
			context.write(new Text(info + "\t" + order),NullWritable.get());
			
		}
	}
	
}
