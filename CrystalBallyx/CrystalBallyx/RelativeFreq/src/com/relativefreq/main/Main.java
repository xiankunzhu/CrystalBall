package com.relativefreq.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.relativefreq.hybrid.Hybrid;
import com.relativefreq.hybrid.Hybrid.Combine;
import com.relativefreq.hybrid.Hybrid.Map;
import com.relativefreq.hybrid.Hybrid.MyPartition;
import com.relativefreq.hybrid.Hybrid.Reduce;
import com.relativefreq.pair.PairCoocur;
import com.relativefreq.pair.PairCoocur.PairCoocurMapper;
import com.relativefreq.pair.PairCoocur.PairCoocurReducer;
import com.relativefreq.pair.PairCoocur.TextPair;
import com.relativefreq.stripe.Stripe;

public class Main {

	public static void main(String[] args) throws Exception {
		/*
	     * Validate that two arguments were passed from the command line.
	     */
	    if (args.length != 4) {
	      System.out.printf("Please use COMMAND LIKE : hadoop jar RelativeFreq.jar CLASSNAME NUMOFREDUCETASK <input data file> <output dir>\n");
	      System.out.printf("CLASSNAME : \n");
	      System.out.printf("PairCoocur : Use find out relative frequency by pair solution\n");
	      System.out.printf("Stripe : Use find out relative frequency by stripe solution\n");
	      System.out.printf("Hybrid : Use find out relative frequency by hybrid solution\n");
	      System.out.printf("NUMOFREDUCETASK : Customize the number of reduce tasks\n");
	      
	      System.exit(-1);
	    }
	    if(args.length == 4){
	    	String classname = args[0];
	    	int numOfReduceTask = Integer.parseInt(args[1]);
	    	if(classname.equals("Hybrid")){
		    	Job job = Job.getInstance(new Configuration(), "Hybrid");
		        job.setJarByClass(Hybrid.class);
		        job.setNumReduceTasks(numOfReduceTask);
	
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
	
		        job.setMapperClass(Hybrid.Map.class);
		        job.setCombinerClass(Hybrid.Combine.class);
		        job.setPartitionerClass(Hybrid.MyPartition.class);
		        job.setReducerClass(Hybrid.Reduce.class);
	
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
	
		        FileInputFormat.addInputPath(job, new Path(args[2]));
				FileOutputFormat.setOutputPath(job, new Path(args[3]));
				
				boolean success = job.waitForCompletion(true);
				System.exit(success ? 0 : 1);
	    	}else if (classname.equals("Stripe")){
	    		Job job = Job.getInstance(new Configuration(), "Stripe");
	            job.setJarByClass(Stripe.class);

	            job.setNumReduceTasks(numOfReduceTask);
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);

	            job.setMapperClass(Stripe.Map.class);
	            job.setCombinerClass(Stripe.Combine.class);
	            job.setReducerClass(Stripe.Reduce.class);

	            job.setInputFormatClass(TextInputFormat.class);
	            job.setOutputFormatClass(TextOutputFormat.class);

	            FileInputFormat.addInputPath(job, new Path(args[2]));
	    		FileOutputFormat.setOutputPath(job, new Path(args[3]));

	    		boolean success = job.waitForCompletion(true);
				System.exit(success ? 0 : 1);
	    	}else if (classname.equals("PairCoocur")){
	    		Configuration conf = new Configuration();
	    		Job job = Job.getInstance(conf, "PairCoocur");
	    		job.setJarByClass(PairCoocur.class);
	    		job.setMapperClass(PairCoocur.PairCoocurMapper.class);
	    		// job.setCombinerClass(WordCountReducer.class);
	    		job.setPartitionerClass(PairCoocur.MyPartition.class);
	    		job.setReducerClass(PairCoocur.PairCoocurReducer.class);
	    		job.setMapOutputKeyClass(PairCoocur.TextPair.class);
	    		job.setMapOutputValueClass(IntWritable.class);
	    		job.setOutputKeyClass(PairCoocur.TextPair.class);
	    		job.setOutputValueClass(DoubleWritable.class);
	    		
	    		job.setNumReduceTasks(numOfReduceTask);
	    		FileInputFormat.addInputPath(job, new Path(args[2]));
	    		FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    		
	    		boolean success = job.waitForCompletion(true);
				System.exit(success ? 0 : 1);
	    	}
	    }
	}

}
