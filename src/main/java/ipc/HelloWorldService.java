package ipc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface HelloWorldService extends VersionedProtocol {
	static final long versionID = 1;
	public String sayHello(String msg);
}
