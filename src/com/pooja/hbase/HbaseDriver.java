

package com.pooja.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDriver {

	public static void main(String[] args) throws IOException {

		//		Initialize config
		Configuration conf = HBaseConfiguration.create();
		
		System.out.println(conf.get("hbase.zookeeper.quorum"));
	    System.out.println(conf.get("hbase.zookeeper.property.clientPort"));

	    //instantiate table
		HTable customer = new HTable(conf, "customer");

		//		perform get operation
		byte[]  rowkey = Bytes.toBytes("poojadube");
		Get get = new Get(rowkey);
		byte[] addrfamily = Bytes.toBytes("addr");
		get.addFamily(addrfamily);

		Result result = customer.get(get);
		
		System.out.println(Bytes.toString(result.getRow()));
		   for (Cell cell : result.rawCells()) {
			   byte[] row = CellUtil.cloneRow(cell);
		        byte[] family = CellUtil.cloneFamily(cell);
		        byte[] column = CellUtil.cloneQualifier(cell);
		        byte[] value = CellUtil.cloneValue(cell);
		        
		        System.out.println(Bytes.toString(row) +"\t" + Bytes.toString(family) + ":" + Bytes.toString(column) + " = " + Bytes.toString(value));
		    }

			for(KeyValue kv : result.raw())
			{
				System.out.println(Bytes.toString( kv.getRow()) );
				System.out.println(Bytes.toString( kv.getFamily()) );
				System.out.println(Bytes.toString( kv.getQualifier()) );									
				System.out.println(Bytes.toString(kv.getValue()));
			}

		   // perform put operation
			rowkey = Bytes.toBytes("pawanagnihotri");
			Put put = new Put(rowkey);
			put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("city"), Bytes.toBytes("simsbury"));
			put.add(Bytes.toBytes("addr"), Bytes.toBytes("state"), Bytes.toBytes("CT"));
			put.add(Bytes.toBytes("order"), Bytes.toBytes("numb"), Bytes.toBytes("50123"));
			put.add(Bytes.toBytes("order"), Bytes.toBytes("date"), Bytes.toBytes("12-30-2016"));
			put.add(Bytes.toBytes("order"), Bytes.toBytes("counter"), Bytes.toBytes(10L));
			customer.put(put);

			//get only address column family
			get = new Get(rowkey);
			result = customer.get(get);
			System.out.println(result.getRow().toString());
			   for (Cell cell : result.rawCells()) {
				   byte[] row = CellUtil.cloneRow(cell);
				   byte[] family = CellUtil.cloneFamily(cell);
			        byte[] column = CellUtil.cloneQualifier(cell);
			        byte[] value = CellUtil.cloneValue(cell);
			        System.out.println(Bytes.toString(row) +"\t" + Bytes.toString(family) + ":" + Bytes.toString(column) + " = " + Bytes.toString(value));
			    }

			   System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("addr"), Bytes.toBytes("state"))));
			   
			   
			   // delete the row 			   
		//	   Delete delete = new Delete(rowkey);
			   
		//	   customer.delete(delete);
			   

			   // scan the table
				byte[]  startrow = Bytes.toBytes("a");
				byte[]  endrow = Bytes.toBytes("z");
				Scan scan = new Scan(startrow, endrow);
				//return one row per rpc call... so you need to set caching to group rows before sending to client
				scan.setCaching(2);
				ResultScanner rs = customer.getScanner(scan);
				
				try{
					   
					for(Result r : rs)
					{
						System.out.println(r);
						String state = Bytes.toString(result.getValue(Bytes.toBytes("addr"), Bytes.toBytes("state")));
						   System.out.println(state);

					}
				}finally{ //close the scanner else cause issues with region server		
					rs.close();
				}
				
				// filter the result
				System.out.println("run filter");
				Scan scanfilter = new Scan();
				//in case you have multiple SingleColumnValueFilters, 
				//you would want the row to pass MUST_PASS_ALL conditions
				//or MUST_PASS_ONE condition.

				FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

				// this is a type of comparision filter.
				//filter based on rowkey
				RowFilter filter_by_key = new RowFilter(CompareOp.EQUAL,new SubstringComparator("t"));

//				filter_by_key.setFilterIfMissing(true);  
		//if you don't want the rows that have the column missing.
		//Remember that adding the column filter doesn't mean that the 
		//rows that don't have the column will not be put into the 
		//result set. They will be, if you don't include this statement. 

		list.addFilter(filter_by_key);
// this is a type of dedicated filter.. always on specific element of the row
		SingleColumnValueFilter filter_by_ordernum = new SingleColumnValueFilter( 
                Bytes.toBytes("order" ),
                Bytes.toBytes("numb"),
                CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes("5010"));

filter_by_ordernum.setFilterIfMissing(true);  
//if you don't want the rows that have the column missing.
//Remember that adding the column filter doesn't mean that the 
//rows that don't have the column will not be put into the 
//result set. They will be, if you don't include this statement. 

list.addFilter(filter_by_ordernum);

// using decorator filter with value 

 list.addFilter(new SkipFilter(new SingleColumnValueFilter( 
        Bytes.toBytes("order" ),
        Bytes.toBytes("numb"),
        CompareOp.EQUAL,
        Bytes.toBytes("6666"))));

				scanfilter.setFilter(list);
				rs = customer.getScanner(scanfilter);
				
					try{
					   
							for(Result r : rs)
							{
								for(KeyValue kv : r.raw())
								{
									System.out.println(Bytes.toString( kv.getRow()) );
									System.out.println(Bytes.toString( kv.getFamily()) );
									System.out.println(Bytes.toString( kv.getQualifier()) );									
									System.out.println(Bytes.toString(kv.getValue()));
								}
						
							}
				}finally{ //close the scanner else cause issues with region server		
					rs.close();
				}
					
// counters
System.out.println("running counters" );

	Increment increament1 = new Increment(Bytes.toBytes("poojadube"));
	
	increament1.addColumn(Bytes.toBytes("order"), Bytes.toBytes("counter"),10L);
	
	 Result resultcounter =  customer.increment(increament1);
	
		try{
			   
				for(Cell kv : resultcounter.rawCells())
				{
			        byte[] family = CellUtil.cloneFamily(kv);
			        byte[] column = CellUtil.cloneQualifier(kv);
			        byte[] value = CellUtil.cloneValue(kv);
			        
			        System.out.println("\t" + Bytes.toString(family) + ":" + Bytes.toString(column) + " = " + Bytes.toLong(value));

				}
		
			
			}finally{ //close the scanner else cause issues with region server		
			rs.close();
			}
				//close table to release resources
		customer.close();
		
	}

}
