package HBook3;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
/**
 * 复制本地文件到hdfs中，使用Progressable只是让你知道正在发生什么事情
 * @author LISHUAI
 *
 */
public class FileCopyWithPorgress {
	public static void main(String[] args) throws Exception {
		String localSrc = args[0];
		String des = args[1];
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(des), conf);
			OutputStream out = fs.create(new Path(des), new Progressable(){
				public void progress(){
					System.out.println(".");
				}
			});
			IOUtils.copyBytes(in, out,4096,false);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}
	}
}
