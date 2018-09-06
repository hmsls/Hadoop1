package HBook3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteToHdfs {
	private static Path path  = null;
	private static FSDataOutputStream fsos = null;
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			
			for(int i = 1;i<5;i++){
				path = new Path("/user/lishuai/a"+i+".txt");
				fsos = fs.create(path);
				fsos.write("aaaaa".getBytes());
			}
			fsos.sync();
			fsos.close();
			System.out.println("over");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
