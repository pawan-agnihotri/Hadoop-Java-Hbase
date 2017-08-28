package com.pooja.hbase.practice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDML {

	static Configuration config = HBaseConfiguration.create();  
	static HTable htable_inv = null;
	static HTable htable_shopping = null;
//Inv row keys
	static byte[] rowkey1 = Bytes.toBytes("Pen");
	static byte[] rowkey2 = Bytes.toBytes("Paper");
	static byte[] rowkey3 = Bytes.toBytes("Marker");
	//Cart row kepys
	static byte[] rowkey4 = Bytes.toBytes("Pooja");
	static byte[] rowkey5 = Bytes.toBytes("Pawan");
	
	
	private static void setup() throws IOException {
		htable_inv = new HTable(config, SetupShoppingCart.table_inv);
		htable_shopping = new HTable(config, SetupShoppingCart.table_shopping);
		
		
	}
	public static void insert() throws IOException {

		//Set up Config and get Htable
		setup();
	//perform put operation
			
		
		//Put in INv
		
		Put put1 = new Put(rowkey1);
		Put put2 = new Put(rowkey2);
		Put put3 = new Put(rowkey3);
		
		// Family qualifier value 
		
		put1.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"), Bytes.toBytes("10000"));
		put1.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"), Bytes.toBytes("2000"));
		
		put2.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"), Bytes.toBytes("200000"));
		put2.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"), Bytes.toBytes("1000"));
		
		put3.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"), Bytes.toBytes("30000"));
		put3.addColumn(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"), Bytes.toBytes("3000"));
		
		htable_inv.put(put1);
		htable_inv.put(put2);
		htable_inv.put(put3);
		
		//Put in Shopping
		
		//byte[] rowkey4 = Bytes.toBytes("Pooja");
		//byte[] rowkey5 = Bytes.toBytes("Pawan");
		
		
		Put put4 = new Put(rowkey4);
		Put put5 = new Put(rowkey5);
		
		put4.addColumn(SetupShoppingCart.cf_cart, rowkey1, Bytes.toBytes("5"));
		put4.addColumn(SetupShoppingCart.cf_cart, rowkey2, Bytes.toBytes("50"));		
		
		put5.addColumn(SetupShoppingCart.cf_cart, rowkey3, Bytes.toBytes("6"));
		put5.addColumn(SetupShoppingCart.cf_cart, rowkey2, Bytes.toBytes("10"));		
		
		List<Put> cartList = new ArrayList<Put>();
		
		cartList.add(put4);
		cartList.add(put5);
		
		htable_shopping.put(cartList);
		
		
			
	}
	
	public static void select() throws IOException {
		//Scan Cart table 
				Scan getCart = new Scan();
				
				getCart.addFamily(SetupShoppingCart.cf_cart);
				
				ResultScanner scannerCart = htable_shopping.getScanner(SetupShoppingCart.cf_cart);
				
				for(Result sc : scannerCart){
								
					System.out.println("Scan Cart table for " + Bytes.toString(SetupShoppingCart.cf_cart) + "Family : " + " " + rowkey1 + " :" +Bytes.toString(sc.getValue(SetupShoppingCart.cf_cart, rowkey1)));
					System.out.println("Scan Cart table for " + Bytes.toString(SetupShoppingCart.cf_cart) + "Family : " + Bytes.toString(sc.getValue(SetupShoppingCart.cf_cart, rowkey2)));
					System.out.println("Scan Cart table for " + Bytes.toString(SetupShoppingCart.cf_cart) + "Family : " + Bytes.toString(sc.getValue(SetupShoppingCart.cf_cart, rowkey3)));
					
				}
				
				//Scan Cart table 
						Scan getInv = new Scan();
						
						getCart.addFamily(SetupShoppingCart.cf_items);
						
						ResultScanner scannerInv = htable_inv.getScanner(SetupShoppingCart.cf_items);
						
						for(Result sc : scannerInv){
										
							System.out.println("Scan Inv table for " + "ITEMS Family : " + "QTY" + " :" +Bytes.toString(sc.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"))));
							System.out.println("Scan Inv table for " + "ITEMS Family : " + "PRICE" + " :" +Bytes.toString(sc.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"))));
											
						}
			
	}
	
	public static void selectRow() throws IOException{
		
		Get getInv_rowkey1 = new Get(rowkey1);
		
		htable_inv.setAutoFlush(false);
		
		Result s = htable_inv.get(getInv_rowkey1);
		
		System.out.println("Inv table output for "+ Bytes.toString(rowkey1) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"))));
		System.out.println("Inv table output for "+ Bytes.toString(rowkey1) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"))));
		
		Get getInv_rowkey2 = new Get(rowkey2);
		
		s = htable_inv.get(getInv_rowkey2);
		
		System.out.println("Inv table output for "+ Bytes.toString(rowkey2) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"))));
		System.out.println("Inv table output for "+ Bytes.toString(rowkey2) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"))));
		
		Get getInv_rowkey3 = new Get(rowkey3);
		
		s = htable_inv.get(getInv_rowkey3);
		
		System.out.println("Inv table output for "+ Bytes.toString(rowkey3) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("QTY"))));
		System.out.println("Inv table output for "+ Bytes.toString(rowkey3) + ": " + Bytes.toString(s.getValue(SetupShoppingCart.cf_items, Bytes.toBytes("PRICE"))));
		
		
		htable_inv.get(getInv_rowkey1);
		htable_inv.get(getInv_rowkey2);
		htable_inv.get(getInv_rowkey3);
		
		htable_inv.flushCommits();
	}
	
	public static void main(String[] args) throws IOException {
		
		HBaseDML.insert();
		HBaseDML.select();
		HBaseDML.selectRow();
	}
	
}