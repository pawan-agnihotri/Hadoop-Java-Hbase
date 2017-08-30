package com.pooja.hbase.practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDDL {

	private static Configuration config = HBaseConfiguration.create();
	
	private static HBaseAdmin admin = null;
	
	private static HTableDescriptor table_Inv = new HTableDescriptor(SetupShoppingCart.table_inv);
	private static  HTableDescriptor table_Shopping = new HTableDescriptor(SetupShoppingCart.table_shopping);
	
	private static HColumnDescriptor cf_Inv = new HColumnDescriptor(SetupShoppingCart.cf_items);
	private static HColumnDescriptor cf_shoppingCart = new HColumnDescriptor(SetupShoppingCart.cf_cart);
 
	public static void createTableOper() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		
		//Instanticate HbaseAdmin with configuration
		admin = new HBaseAdmin(config);
		
		//settign up column family parameters on INv table 
		cf_Inv.setMaxVersions(4);
		cf_Inv.setMinVersions(2);
		
		//Add column family to Htable description 
		table_Inv.addFamily(cf_Inv);
		table_Shopping.addFamily(cf_shoppingCart);
		
		// if table doesnot exist them create table via HBaseAdmin
		if(!admin.tableExists(SetupShoppingCart.table_inv)){
			admin.createTable(table_Inv);
		}
		if(!admin. tableExists(SetupShoppingCart.table_shopping)){
			admin.createTable(table_Shopping);
		}
		 for(TableName t : admin.listTableNames()){
			 System.out.println("Table Name is  : "+ Bytes.toString(t.getName()));
		 }
		System.out.println("List tables : " +  admin.listTableNames());
	
			
	}
	
	public static void dropTable() throws IOException{
		admin = new HBaseAdmin(config);
		
		System.out.println("List tables : " +  admin.listTableNames());
	
		
		System.out.println("deleting or dropping tables after disabling them : ");
		admin.disableTable(SetupShoppingCart.table_inv);
		admin.disableTable(SetupShoppingCart.table_shopping);
		
		admin.deleteTable(SetupShoppingCart.table_inv);
		admin.deleteTable(SetupShoppingCart.table_shopping);
		System.out.println("List tables : " +  admin.listTableNames());
		
		
	}
	
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		
		
		HbaseDDL.createTableOper();

		//HbaseDDL.dropTable();
		System.out.println("Closing Admin to release resources.");
		HbaseDDL.admin.close();
				
		}
	
	
	
}
