package HBook5;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationTest {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource("conf-1.xml");
	}
}
