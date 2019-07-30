package com.amazonaws.dsps192;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3 {

	public static class Mapper3 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] Entry = itr.nextToken().split("\t");
				// entry Structure:
				// year TAB w1 TAB w2 TAB npmi

				Double npmi, decadeNpmi, minPmi, relMinPmi;
				String year, w1, w2;
				year = Entry[0];
				w1 = Entry[1];
				w2 = Entry[2];
				npmi = Double.valueOf(Entry[3]);

				decadeNpmi = Double.valueOf(context.getConfiguration().get("NPMI_" + year, "0"));
				decadeNpmi = decadeNpmi / 100000.0;
				minPmi = Double.valueOf(context.getConfiguration().get("minPmi", "0"));
				relMinPmi = Double.valueOf(context.getConfiguration().get("relMinPmi", "0"));

				Text newKey = new Text(year + "\t" + npmi);
				Text newValue = new Text(w1 + "\t" + w2);

				if (npmi >= minPmi) {
					context.write(newKey, newValue);
				} else if (decadeNpmi != 0 && ((npmi / decadeNpmi) >= relMinPmi)) {
					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	public static class yearNpmiComperator extends WritableComparator {
		protected yearNpmiComperator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;
			String[] key1 = k1.toString().split("\t");
			String[] key2 = k2.toString().split("\t");
			String year1, year2, npmi1, npmi2;
			year1 = key1[0];
			year2 = key2[0];
			npmi1 = key1[1];
			npmi2 = key2[1];
			if (year1.compareTo(year2) == 0)
				return -(npmi1.compareTo(npmi2));
			return year1.compareTo(year2);
		}
	}

	public static class DecadePartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] keyStr = key.toString().split("\t");
			String year = keyStr[0];
			Integer decade = Integer.valueOf(year) / 10;
			int partitioner = (decade - 150) % numReduceTasks;
			if (partitioner < 0) {
				return partitioner * -1;
			}
			return partitioner;
		}
	}

}
