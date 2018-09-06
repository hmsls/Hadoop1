package HBook3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * 获取FileSystem的实例的方法，在FileSystem类中
 * public static FileSystem get(Configuration conf) throws IOE...
 * 	返回默认文件系统，在core-site.xml中指定的，如果没有指定则是返回本地文件系统
 * public static FileSystem get(URI uri,Configuration conf) throws IOE...
 * 	返回通过给定的uri方案和权限来指定文件系统
 * public static FileSystem get(URI,uri,Configuration conf,String user) throws IOE...
 * 	作为给定用户来访访问文件系统
 * 
 * 获取本地文件系统LocalFileSystem，在FileSystem类中。
 * 	public static LocalFileSystem getLocal(Configuration conf) throws IOE...
 * 
 * 通过open函数来获取文件的输入流。
 * @author LISHUAI
 *
 */
public class FileSystemCat {
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		InputStream is = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			
			is = fs.open(new Path(uri));
			IOUtils.copyBytes(is, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(is);
		}
	}
}
