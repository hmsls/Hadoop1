package ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {
	public static void main(String[] args){
		try {
			Configuration conf = new Configuration();
			Server server = new RPC.Builder(conf).setProtocol(HelloWorldService.class)
					.setInstance(new HelloWorldServiceImpl()).setBindAddress("localhost")
					.setNumHandlers(2)
					.setPort(8888).build();
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
