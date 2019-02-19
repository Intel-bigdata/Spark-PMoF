package org.apache.spark.storage.pmof;
 
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.io.*;
 
public class PersistentMemoryMetaHandler {

  private static String url = "jdbc:sqlite:/tmp/spark_shuffle_meta.db";

  PersistentMemoryMetaHandler(String root_dir) {
    createTable(root_dir);
  }

  public void createTable(String root_dir) {
    String sql = "CREATE TABLE IF NOT EXISTS metastore (\n"
                + "	shuffleId text PRIMARY KEY,\n"
                + "	device text NOT NULL,\n"
                + " UNIQUE(shuffleId, device)\n"
                + ");\n";

    url = "jdbc:sqlite:" + root_dir + "/spark_shuffle_meta.db";

    try {
      Connection conn = DriverManager.getConnection(url);
      Statement stmt = conn.createStatement();
      stmt.execute(sql);

      sql = "CREATE TABLE IF NOT EXISTS devices (\n"
                + "	device text UNIQUE\n"
                + ");";
      stmt.execute(sql);
      conn.close();
    } catch (SQLException e) {
      System.err.println("createTable failed:" + e.getMessage());
      System.exit(-1);
    }
    System.out.println("Metastore DB connected: " + url);
  }

  public void insertRecord(String shuffleId, String device) {
    String sql = "INSERT OR IGNORE INTO metastore(shuffleId,device) VALUES('" + shuffleId + "','" + device + "');\n";
    sql += "INSERT OR IGNORE INTO devices(device) VALUES('" + device + "');";
 
    try {
      Connection conn = DriverManager.getConnection(url);
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.close();
    } catch (SQLException e) {
      System.err.println("insertRecord failed:" + e.getMessage());
      System.exit(-1);
    }
  }

  public String getShuffleDevice(String shuffleId){
    String sql = "SELECT device FROM metastore where shuffleId = ?";
    String res = "";
    
    try {
      Connection conn = DriverManager.getConnection(url);
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, shuffleId);
      ResultSet rs = pstmt.executeQuery();
      if (rs != null) {
        res = rs.getString("device"); 
      }
      conn.close();
    } catch (SQLException e) {
    }
    return res;
  }

  public String getUnusedDevice(ArrayList<String> full_device_list){
    String sql = "SELECT device FROM devices";
    ArrayList<String> device_list = new ArrayList<String>();
    String device = "";
    
    try {
      Connection conn = DriverManager.getConnection(url);
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        device_list.add(rs.getString("device"));
      }

      full_device_list.removeAll(device_list);
      if (full_device_list.size() == 0) {
        System.err.println("remains zero unused device");
        System.exit(-1);
      }
      device = full_device_list.get(0);

      sql = "INSERT OR IGNORE INTO devices(device) VALUES('" + device + "')\n";

      stmt.executeUpdate(sql);
      conn.close();
    } catch (SQLException e) {
      System.err.println("getUnusedDevice insert device " + device + "failed: " + e.getMessage());
      System.exit(-1);
    }
    System.out.println("Metastore DB: get unused device, should be " + device + ".");
    return device;
  }

  public void remove() {
    new File(url).delete();
  }

  public static void main(String[] args) {
    PersistentMemoryMetaHandler pmMetaHandler = new PersistentMemoryMetaHandler("/tmp/");
    /*System.out.println("create table");
    pmMetaHandler.createTable();*/
    /*System.out.println("insert record");
    pmMetaHandler.insertRecord("shuffle_0_1_0", "/dev/dax0.0");
    pmMetaHandler.insertRecord("shuffle_0_2_0", "/dev/dax0.0");
    pmMetaHandler.insertRecord("shuffle_0_3_0", "/dev/dax1.0");
    pmMetaHandler.insertRecord("shuffle_0_4_0", "/dev/dax1.0");
    */
    System.out.println("get shuffle device");
    String dev = pmMetaHandler.getShuffleDevice("shuffle_0_85_0");
    System.out.println("shuffle_0_85_0 uses device: " + dev);
    
    /*ArrayList<String> device_list = new ArrayList<String>();
    device_list.add("/dev/dax0.0");
    device_list.add("/dev/dax1.0");
    device_list.add("/dev/dax2.0");
    device_list.add("/dev/dax3.0");
    System.out.println("get unused device");
    dev = pmMetaHandler.getUnusedDevice(device_list);
    System.out.println("get currently unused device: " + dev);*/
  }
}
