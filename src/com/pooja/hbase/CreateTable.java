package com.pooja.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;

public class CreateTable {
      
   public static void main(String[] args) throws IOException, InterruptedException { 

      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();

      // Instantiating HbaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(con);

      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor("pooja_employee");

      // Adding column families to table descriptor
      HColumnDescriptor personal = new HColumnDescriptor("personal");
      HColumnDescriptor professional = new HColumnDescriptor("professional");
      //column family attribute
      personal.setMaxVersions(5);
      
      tableDescriptor.addFamily(personal);
      tableDescriptor.addFamily(professional);

      // Execute the table through admin
      if(!admin.tableExists(TableName.valueOf("pooja_employee")))
      {
          admin.createTable(tableDescriptor);
          System.out.println(" Table created ");
   	  
      }
      // Getting all the list of tables using HBaseAdmin object
      HTableDescriptor[] tableDesc = admin.listTables();

      // printing all the table names.
      for (int i=0; i<tableDesc.length;i++ ){
         System.out.println(tableDesc[i].getNameAsString());
      }
      System.out.println("please enter thread sleep time in milli seconds");
      try{
      int millsec = Integer.valueOf(System.in.read());
      System.out.println("millsec : " + millsec);
      Thread.sleep(millsec);
      }catch (Exception e){
    	System.out.println("invalid input: " + e.getMessage());
    	System.out.println("took default input: 10 secs");
    	Thread.sleep(10000);
      }
  // delete the table
      if(admin.tableExists(TableName.valueOf("pooja_employee")))
      {
    	  admin.disableTable(TableName.valueOf("pooja_employee"));
    	  admin.deleteTable(TableName.valueOf("pooja_employee"));
      }
      admin.close();
   }
}