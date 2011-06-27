package gov.llnl.demo;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;

public class ScanRows {

  public static void main(String[] args) {

    try {
      byte[] table = Bytes.toBytes(args[0]);

      Configuration config = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(config);

      if (!admin.tableExists(table)) {
        System.out.println("table does not exist: " + args[0]);
        return;
      }

      // get row from table
      HTable myTable = new HTable(config, table);

      System.out.println("scanning full table:");
      Scan s = null;
      s = new Scan();

      ResultScanner scanner = myTable.getScanner(s);
      countRows(scanner);

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Just a generic print function given an iterator.
   * 
   * @param scanner
   * @throws IOException
   */
  public static void countRows(ResultScanner scanner) throws IOException {
    // iterates through and prints all rows
    int rows = 0;
    for (Result row = scanner.next(); row != null; row = scanner.next()) {
      rows++;
      System.out.println("\nresults for row \"" + toString(row.getRow()) + "\"");
      // print column/values from row
      for (KeyValue kv : row.list()) {
        System.out.println(toString(kv.getFamily()) + ":" + toString(kv.getQualifier()) + " == \""
            + toString(kv.getValue()) + "\"");
      }
    }
    System.out.println("---------");
    System.out.println("total rows: " + rows);
  }

  public static String toString(byte[] s) {
    return new String(s, Charsets.UTF_8);
  }

}
