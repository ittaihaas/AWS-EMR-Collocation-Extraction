package com.amazonaws.dsps192;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepsRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String ngramData = args[1];
		String s3bucket = args[2];
		boolean exitCode;

		/**
		 * configuration for job0
		 */
		Configuration conf0 = new Configuration();
		conf0.set("minPmi", args[3]);
		conf0.set("relMinPmi", args[4]);

		Job job0 = Job.getInstance(conf0, "job0");
		job0.setJarByClass(StepsRunner.class);
		job0.setMapperClass(Step0.Mapper0.class);
		job0.setCombinerClass(Step0.Reducer0.class);
		job0.setReducerClass(Step0.Reducer0.class);
		job0.setOutputKeyClass(Text.class);
		job0.setOutputValueClass(Text.class);
		job0.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job0, new Path(ngramData));
		FileOutputFormat.setOutputPath(job0, new Path(s3bucket + "output0"));
		exitCode = job0.waitForCompletion(true);
		if (!exitCode)
			System.exit(1);
		System.out.println("job0 complete");

		/**
		 * configuration for job1
		 */
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "job1");
		job1.setJarByClass(StepsRunner.class);
		job1.setMapperClass(Step1.Mapper1.class);
		job1.setCombinerClass(Step1.CWcombiner.class);
		job1.setReducerClass(Step1.Reducer1.class);
		job1.setPartitionerClass(Step1.CWPartitioner.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(s3bucket + "output0"));
		FileOutputFormat.setOutputPath(job1, new Path(s3bucket + "output1"));
		exitCode = job1.waitForCompletion(true);
		if (!exitCode)
			System.exit(1);
		System.out.println("job1 complete");

		/**
		 * configuration for job2
		 */
		Configuration conf2 = new Configuration();
		Counters jobCounters;
		jobCounters = job1.getCounters();
		for (CounterGroup counterGroup : jobCounters)
			for (Counter counter : counterGroup)
				if (counter.getName().contains("N_")) 
					conf2.set(counter.getName(), "" + counter.getValue());


		Job job2 = Job.getInstance(conf2, "job2");
		job2.setJarByClass(StepsRunner.class);
		job2.setMapperClass(Step2.Mapper2.class);
		job2.setCombinerClass(Step2.CWcombiner.class);
		job2.setReducerClass(Step2.Reducer2.class);
		job2.setPartitionerClass(Step2.CWPartitioner.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(s3bucket + "output1"));
		FileOutputFormat.setOutputPath(job2, new Path(s3bucket + "output2"));
		exitCode = job2.waitForCompletion(true);
		if (!exitCode)
			System.exit(1);
		System.out.println("job2 complete");

		/**
		 * configuration for job3
		 */
		Configuration conf3 = new Configuration();
		conf3.set("minPmi", args[3]);
		conf3.set("relMinPmi", args[4]);
		jobCounters = job2.getCounters();
		for (CounterGroup counterGroup : jobCounters)
			for (Counter counter : counterGroup)
				if (counter.getName().contains("NPMI_"))
					conf3.set(counter.getName(), "" + counter.getValue());

		Job job3 = Job.getInstance(conf3, "job3");
		job3.setJarByClass(StepsRunner.class);
		job3.setMapperClass(Step3.Mapper3.class);
		job3.setReducerClass(Step3.Reducer3.class);
		job3.setPartitionerClass(Step3.DecadePartitioner.class);
		job3.setSortComparatorClass(Step3.yearNpmiComperator.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(s3bucket + "output2"));
		FileOutputFormat.setOutputPath(job3, new Path(s3bucket + "output3"));
		exitCode = job3.waitForCompletion(true);
		if (!exitCode)
			System.exit(1);
		System.out.println("job3 complete");
		System.exit(0);
	}
}
