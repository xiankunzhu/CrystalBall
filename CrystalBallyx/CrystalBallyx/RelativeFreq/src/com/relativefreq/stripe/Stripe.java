package com.relativefreq.stripe;

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

public class Stripe {
    public static void main(String[] args) throws Exception {
    	Job job = Job.getInstance(new Configuration(), "Stripe");
        job.setJarByClass(Stripe.class);

        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            for (int i = 0; i < words.length; i++) {
            		String word = words[i];
                    java.util.Map<String, Integer> stripe = new HashMap<>();

                    for (int j = 1; j<= 3 && i + j < words.length ; j ++) {
                    		String term = words[i+j];
                            Integer count = stripe.get(term);
                            stripe.put(term, (count == null ? 0 : count) + 1);
                    }

                    StringBuilder Stripetr = new StringBuilder();
                    for (java.util.Map.Entry<String, Integer> entry : stripe.entrySet()) {
                        Stripetr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
                    }

                    if (!stripe.isEmpty()) {
                        context.write(new Text(word), new Text(Stripetr.toString()));
                    }
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            java.util.Map<String, Integer> stripes = new HashMap<>();
            for (Text value : values) {
                String[] Stripe = value.toString().split(",");

                for (String termCountStr : Stripe) {
                    String[] termCount = termCountStr.split(":");
                    String term = termCount[0];
                    int count = Integer.parseInt(termCount[1]);

                    Integer countSum = stripes.get(term);
                    stripes.put(term, (countSum == null ? 0 : countSum) + count);
                }
            }

            StringBuilder Stripetr = new StringBuilder();
            for (java.util.Map.Entry<String, Integer> entry : stripes.entrySet()) {
                Stripetr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            context.write(key, new Text(Stripetr.toString()));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            java.util.Map<String, Integer> stripe = new HashMap<>();
            double totalCount = 0;

            for (Text value : values) {
                String[] Stripe = value.toString().split(",");

                for (String termCountStr : Stripe) {
                    String[] termCount = termCountStr.split(":");
                    String term = termCount[0];
                    int count = Integer.parseInt(termCount[1]);

                    Integer countSum = stripe.get(term);
                    stripe.put(term, (countSum == null ? 0 : countSum) + count);

                    totalCount += count;
                }
            }
            StringBuilder stripeStr = new StringBuilder();
            stripeStr.append("<");
            for (java.util.Map.Entry<String, Integer> entry : stripe.entrySet()) {
            	stripeStr.append("(").append(entry.getKey()).append(",").append((int)entry.getValue()/totalCount).append(")");
            }
            stripeStr.append(">");
            context.write(key, new Text(stripeStr.toString()));
        }
    }
    
}