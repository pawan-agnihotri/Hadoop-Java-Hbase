package com.pooja.hbase.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class SetupShoppingCart {

	public static byte[] table_inv = Bytes.toBytes("INVENTORY");
	
	public static byte[] table_shopping = Bytes.toBytes("SHOPPING_CART");
	
	public static byte[] cf_items = Bytes.toBytes("ITEMS");
	
	public static byte[] cf_cart = Bytes.toBytes("CART");
	
	//public static Configuration config = HBaseConfiguration.create();
	
	//public static HBaseAdmin admin = null;
	
	
}
