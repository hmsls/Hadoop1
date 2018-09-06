package HBook3;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
/**
 * 这个类可以通过运行时配置参数进程测试，参数为
 * hdfs://60.24.64.25/hechao/c_pd_prod/000000_0
 * @author LISHUAI
 *
 */
public class GetDataFromUrl {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		InputStream in = null;
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}
				
	}
}
