package com.pooja.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pooja.counter.DisplayNameDriver;
import com.pooja.counter.DisplayNameMapper;
import com.pooja.counter.DisplayNameReducer;

public class HbaseClient {
	
	public static void main(String[] args) throws Exception {

	    /*
	     * Validate that two arguments were passed from the command line.
	     */
	    if (args.length != 1) {
	      System.out.printf("Usage: StubDriver <output dir>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Configuration config = HBaseConfiguration.create();
	    Job job = new Job(config);
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running 
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(HbaseDriver.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Hbase Driver");
	// mapper and reducer

	    Scan scan = new Scan();
	    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
	    scan.setCacheBlocks(false);  // don't set to true for MR jobs
	    // set other scan attrs

	    TableMapReduceUtil.initTableMapperJob(
	    	Bytes.toBytes("customer"),        // input table
	    	scan,               // Scan instance to control CF and attribute selection
	    	CustomerHbaseMapper.class,     // mapper class
	    	Text.class,         // mapper output key
	    	Text.class,  // mapper output value
	    	job);
	    
	    job.setReducerClass(CustomerFileReducer.class);    // reducer class
	    
	    job.setNumReduceTasks(1);    // at least one, adjust as required
	    
	    // input and output file paths    
	    
	    // source is hbase so commenting the output
	    //FileInputFormat.addInputPath(job, new Path(args[0]));
	    //lets right it on hdfs
	     FileOutputFormat.setOutputPath(job, new Path(args[0]));
	     
	 //  set output keys
	     
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);
	     
	     
	     /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	     job.waitForCompletion(true);
	}

}
