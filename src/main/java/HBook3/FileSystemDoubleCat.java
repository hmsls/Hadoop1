package HBook3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * seek方法开销大，慎用
 * @author LISHUAI
 *
 */
public class FileSystemDoubleCat {
	public static void main(String[] args) {
		FSDataInputStream fsis = null;
		try {
			String uri = args[0];
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			fsis  =  fs.open(new Path(uri));
			IOUtils.copyBytes(fsis, System.out, 4096,false);
			fsis.seek(0);
			IOUtils.copyBytes(fsis, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(fsis);
		}
	}
}
