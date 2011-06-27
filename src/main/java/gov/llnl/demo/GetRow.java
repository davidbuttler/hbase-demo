package gov.llnl.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;

public class GetRow {

  public static void main(String[] args) {

    try {
      byte[] table = Bytes.toBytes(args[0]);
      byte[] rowId = Bytes.toBytes(args[1]);
      
      Configuration config = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(config);

      if (!admin.tableExists(table)) {
        System.out.println("table does not exist: " + args[0]);
        return;
      }

      // get row from table
      HTable myTable = new HTable(config, table);
      Get get = new Get(rowId);
      Result row = myTable.get(get);
      
      System.out.println("results for row \""+args[1]+"\"");
      // print column/values from row
      for (KeyValue kv : row.list()) {
        System.out.println(toString(kv.getFamily()) +":"+toString(kv.getQualifier()) +" == \""+toString(kv.getValue())+"\"");
      }
      
      
      
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }

  public static String toString(byte[] s) {
    return new String(s, Charsets.UTF_8);
  }
}
