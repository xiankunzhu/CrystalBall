package com.relativefreq.hybrid;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Hybrid {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Hybrid");
        job.setJarByClass(Hybrid.class);

        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setPartitionerClass(MyPartition.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//        job.waitForCompletion(true);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    	java.util.Map<String, Integer> pairs = new HashMap<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            for (int i = 0;i < words.length; i++) {
                    for (int j = i+1; j < words.length; j++){
                    	if(words[i].equals(words[j])) return;
                    	String keyOfPair = words[i] + "," + words[j];
                    	Integer count = pairs.get(keyOfPair);
                    	pairs.put(keyOfPair, (count == null ? 0 : count) + 1);
                    }
            }
        }
		
        @Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
        	if (!pairs.isEmpty()) {
        		for (java.util.Map.Entry<String, Integer> entry : pairs.entrySet()) {
                    context.write(new Text((String)entry.getKey()), new Text(entry.getValue().toString()));
                }
        	}
			super.cleanup(context);
		}
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(count)));
        }
    }
    
	public static class MyPartition extends
			HashPartitioner<Text, Text> {

		public int getPartition(Text pairKey, Text value,
				int numReduceTasks) {
			String key = pairKey.toString().split(",")[0];
			return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        java.util.Map<String, Double> stripes = new HashMap<>();
        double marginal = 0;
        String keyStr = null;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	
            String pairkeyStr = key.toString();
            String keyStrs[] = pairkeyStr.split(",");
            String keyW = keyStrs[0];
            String keyU = keyStrs[1];
            double count = 0;
            
            
            if(!keyW.equals(keyStr)){
            	if(!stripes.isEmpty()){
//            		for (java.util.Map.Entry<String, Double> entry : stripes.entrySet()) {
//            			stripes.put(entry.getKey().toString(), Double.valueOf((double)(entry.getValue())/marginal));
//    	            }
        			StringBuilder stripeStr = new StringBuilder();
        			stripeStr.append("<");
    	            for (java.util.Map.Entry<String, Double> entry : stripes.entrySet()) {
    	            	stripeStr.append("(").append(entry.getKey()).append(",").append(entry.getValue()/marginal).append(")");
    	            }
    	            stripeStr.append(">");
    	            context.write(new Text(keyStr), new Text(stripeStr.toString()));
    	            
    	            
            	}
            	//reset
        		stripes.clear();
        		keyStr = keyW;
        		marginal = 0;    
//        		System.out.println("###"+ keyStr+ "," + keyW + "," + keyU);
            }
            for (Text value : values) {
            	count += Integer.parseInt(value.toString());
            }
        	marginal += count;
        	stripes.put(keyU, count);
//        	System.out.println("@@@" + keyStr+ "," + keyW + "," + keyU);
        	
        }

        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
//        	for (java.util.Map.Entry<String, Double> entry : stripes.entrySet()) {
//    			stripes.put(entry.getKey().toString(), Double.valueOf((double)(entry.getValue())/marginal));
//            }
        	StringBuilder stripeStr = new StringBuilder();
			stripeStr.append("<");
            for (java.util.Map.Entry<String, Double> entry : stripes.entrySet()) {
            	stripeStr.append("(").append(entry.getKey()).append(",").append(entry.getValue()/marginal).append(")");
            }
            stripeStr.append(">");
            context.write(new Text(keyStr), new Text(stripeStr.toString()));
        }
        
    }
}