import java.io.IOException;
import java.io.FileReader;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseTestConnectionK {
	
	public static void main(String ar[]) throws IOException {
		String results = "";
		String krb5_conf = "";
		String useSubjectCredsOnly = "";
		String hbase_path = "";
		String hdfs_path = "";
		String principal = "";
		String keytab = "";
		
		try {
			Properties properties = new Properties();

			try(FileReader fileReader = new FileReader("config.properties")){
			    properties.load(fileReader);
			} catch (IOException e) {
			    e.printStackTrace();
			}
 
			Date time = new Date(System.currentTimeMillis());
 
			// get the property value and print it out
			krb5_conf = properties.getProperty("java.security.krb5.conf");
			useSubjectCredsOnly = properties.getProperty("useSubjectCredsOnly");
			hbase_path = properties.getProperty("hbase_path");
			hdfs_path = properties.getProperty("hdfs_path");
			principal = properties.getProperty("principal");
			keytab = properties.getProperty("keytab");
			hbaseTable = properties.getProperty("hbase_table");
 
			results = "Using to connect = " + krb5_conf + ", " + hbase_path + ", " + hdfs_path+ ", " + principal+ ", " + keytab;
			System.out.println(results + "\nProgram Ran on " + time);
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} 
		
		//System Properties (Change Path/Properties according to env)
		
		//copy krb5.conf from cluster
		System.setProperty("java.security.krb5.conf", krb5_conf);
		System.setProperty("javax.security.auth.useSubjectCredsOnly", useSubjectCredsOnly);
		
		//Configuration (Change Path/Properties according to env)
		Configuration configuration = HBaseConfiguration.create();
		//org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create(); 
		configuration.set("hadoop.security.authentication", "Kerberos");
		
		//copy hbase-site.xml and hdfs-site.xml from cluster and set paths
		configuration.addResource(new Path("file://"+hbase_path));
		configuration.addResource(new Path("file://"+hdfs_path));
		UserGroupInformation.setConfiguration(configuration);
		
		//User information (Change Path/Properties according to env)
		UserGroupInformation.loginUserFromKeytab(principal, keytab);
		
		//Connection
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
		//Test Connection
		System.out.println(connection.getAdmin().isTableAvailable(TableName.valueOf("SYSTEM.STATS")));
		
		Scan scan1 = new Scan();
		Table table = connection.getTable(TableName.valueOf(hbaseTable));
		ResultScanner scanner = table.getScanner(scan1);
		// Reading values from scan result
      	for (Result result = scanner.next(); result != null; result = scanner.next()) {
      		System.out.println("Found row : " + result);
      	}

      	//closing the scanner
      	scanner.close();
	}
}
