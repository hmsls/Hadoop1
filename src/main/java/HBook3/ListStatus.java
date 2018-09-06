package HBook3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class ListStatus {
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			
			Path[] paths = new Path[args.length];
			for(int i = 0;i<paths.length;i++){
				paths[i] = new Path(args[i]);
			}
//			for(Path p : paths){
//				System.out.println(p);
//			}
			//通过上面的也可以得出文件的路径，但是通过下面的转换，中间的FileStatus
//			可以得到文件的更多信息。
			FileStatus[] stats = fs.listStatus(paths);
			for(FileStatus s:stats){
				FsPermission fp = s.getPermission();
				String p = fp.toString();
				System.out.println(p);
			}
			Path[] listedPaths = FileUtil.stat2Paths(stats);
			for(Path p : listedPaths){
				System.out.println(p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
