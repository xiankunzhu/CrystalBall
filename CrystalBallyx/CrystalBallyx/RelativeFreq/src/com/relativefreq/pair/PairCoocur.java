package com.relativefreq.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class PairCoocur {
	public static class TextPair implements WritableComparable<TextPair> {
		private Text leftKey;
		private Text rightKey;

		public TextPair() {
			set(new Text(), new Text());
		}

		public TextPair(String leftKey, String rightKey) {
			set(new Text(leftKey), new Text(rightKey));
		}

		public TextPair(Text leftKey, Text rightKey) {
			set(leftKey, rightKey);
		}

		public void set(Text leftKey, Text rightKey) {
			this.leftKey = leftKey;
			this.rightKey = rightKey;
		}

		public Text getLeftKey() {
			return this.leftKey;
		}

		public Text getRightKey() {
			return this.rightKey;
		}

		public void write(DataOutput out) throws IOException {
			leftKey.write(out);
			rightKey.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			leftKey.readFields(in);
			rightKey.readFields(in);
		}

		public int hashCode() {
			return leftKey.hashCode() * 163 + rightKey.hashCode();
		}

		public boolean equals(Object o) {
			if (o instanceof TextPair) {
				TextPair tp = (TextPair) o;
				return leftKey.equals(tp.leftKey)
						&& rightKey.equals(tp.rightKey);
			}
			return false;
		}

		public String toString() {
			return leftKey + " " + rightKey;
		}

		public int compareTo(TextPair other) {
			int cmp = leftKey.compareTo(other.leftKey);
			if (cmp != 0) {
				return cmp;
			}
			if (this.getRightKey().equals("*")) {
				return 1;
			}
			if (other.getRightKey().equals("*")) {
				return -1;
			}
			return rightKey.compareTo(other.rightKey);
		}
	}

	public static class PairCoocurMapper extends
			Mapper<LongWritable, Text, TextPair, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();
		private Text leftKey = new Text();
		private Text rightKey = new Text();
		private TextPair pairKey = new TextPair();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\s");
			for (int i = 0; i < tokens.length-1; i++) {
				for (int j = i + 1; j < tokens.length; j++) {
					if (tokens[j].equals(tokens[i]))
						break;
					// System.out.println(tokens[i]+","+tokens[j]);
					// word.set(tokens[i]+" "+"*");
					leftKey.set(tokens[i]);
					rightKey.set("*");
					pairKey.set(leftKey, rightKey);
					context.write(pairKey, one);
					// word.set(tokens[i]+" "+tokens[j]);
					leftKey.set(tokens[i]);
					rightKey.set(tokens[j]);
					pairKey.set(leftKey, rightKey);
					context.write(pairKey, one);
				}

			}
		} // map
	} // mapper

	// customized partitioner
	/*
	* public class HashPartitioner<K, V> extends Partitioner<K, V> {
	* 
	* // Use {@link Object#hashCode()} to partition. public int getPartition(K
	* key, V value, int numReduceTasks) { return (key.hashCode() &
	* Integer.MAX_VALUE) % numReduceTasks; } }
	*/

	public static class MyPartition extends
			HashPartitioner<TextPair, IntWritable> {
		private Text leftKey = new Text();

		public int getPartition(TextPair pairKey, IntWritable value,
				int numReduceTasks) {
			leftKey = pairKey.getLeftKey();
			return (leftKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	
	
	public static class PairCoocurReducer extends
			Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
		private IntWritable totalLeftKeyCount = new IntWritable(0);
		private DoubleWritable relativeFreq = new DoubleWritable();
		// private Text lastWord = new Text("NOT_SET");
		private Text starFlag = new Text("*");

		// private Text currentWord = new Text("NOT_SET");

		public void reduce(TextPair pairKey, Iterable<IntWritable> values,
				Context context) throws IOException,
		InterruptedException {
			if (pairKey.getRightKey().equals(starFlag)) {
				int leftKeyCount = 0;
				for (IntWritable val : values) {
					leftKeyCount  += val.get();
				}
				totalLeftKeyCount.set(leftKeyCount);
				//System.out.println("@@@"+pairKey.getLeftKey().toString()+","+pairKey.getRightKey().toString()+" "+leftKeyCount);
			} else {
				int pairCount = 0; // getTotalCount(values);
				for (IntWritable val : values) {
					pairCount += val.get();
				}
				//System.out.println((double) pairCount / leftKeyCount);
				//System.out.println("###"+pairKey.getLeftKey().toString()+","+pairKey.getRightKey().toString()+" "+(double)pairCount/totalLeftKeyCount.get());
				relativeFreq.set((double) pairCount / totalLeftKeyCount.get());
				context.write(pairKey, relativeFreq);
			}
		} // reduce
	} // reducer
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PairCoocur");
		job.setJarByClass(PairCoocur.class);
		job.setMapperClass(PairCoocurMapper.class);
		// job.setCombinerClass(WordCountReducer.class);
		job.setPartitionerClass(MyPartition.class);
		job.setReducerClass(PairCoocurReducer.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.submit();
		boolean sucess = job.waitForCompletion(true);
		System.exit(sucess? 0:1);
	}

}