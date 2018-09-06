package HBook3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ShowFileStatusTest {
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			Path file = new Path("/user/lishuai");
			FileStatus fstat = fs.getFileStatus(file);
			String path = fstat.getPath().toUri().getPath();
			boolean isdir  = fstat.isDir();
			long len = fstat.getLen();
			long modiTime= fstat.getModificationTime();
			short replica = fstat.getReplication();
			long blocksize = fstat.getBlockSize();
			String owner = fstat.getOwner();
			String group = fstat.getGroup();
			System.out.println(path+"---"+isdir+"---"+len+"---"+modiTime+"---"+replica+"---"+blocksize+"---"+owner+"---"+group);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
