package gov.llnl.demo;

import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_BLOCKCACHE;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_BLOOMFILTER;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_IN_MEMORY;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_TTL;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_VERSIONS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

  public static void main(String[] args) {

    try {
      String myTableName = args[0];

      Configuration conf = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (admin.tableExists(Bytes.toBytes(myTableName)))
        return;

      // add column families
      HTableDescriptor tableDesc = new HTableDescriptor(Bytes.toBytes(myTableName));
      for (int i = 1; i < args.length; i++) {
        tableDesc.addFamily(new HColumnDescriptor(args[i].getBytes(), DEFAULT_VERSIONS, "GZ", DEFAULT_IN_MEMORY,
            DEFAULT_BLOCKCACHE, DEFAULT_TTL, DEFAULT_BLOOMFILTER));
      }

      // create table
      admin.createTable(tableDesc);

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
