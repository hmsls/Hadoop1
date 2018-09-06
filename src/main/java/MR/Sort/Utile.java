package MR.Sort;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utile {
	public static String getGrp(String grp,int hash) throws Exception{
		String hostname = InetAddress.getLocalHost().getHostName();
		long time = System.nanoTime();
		return "["+hostname+"]"+hash+":"+time+":"+grp;
	}
}
