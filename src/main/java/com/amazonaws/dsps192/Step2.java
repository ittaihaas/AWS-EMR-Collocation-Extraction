package com.amazonaws.dsps192;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2 {
	
	public enum pmiCounter {
		NPMI_ALL;
	};

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private String year, w1, w2, count, cw1;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] ngramEntry = itr.nextToken().split("\t");
				// entry structure:
				// year TAB w1 TAB w2 TAB count TAB cw1

				year = ngramEntry[0];
				w1 = ngramEntry[1];
				w2 = ngramEntry[2];
				count = ngramEntry[3];
				cw1 = ngramEntry[4];

				// key-value for counting c(w2)
				Text countKey = new Text(year + "\t" + w2 + "\t" + "*");
				Text countValue = new Text(count);

				// key-value to match c(w2) with the rest of the N-gram data
				Text w2Key = new Text(year + "\t" + w2 + "\t" + "**");
				Text w2Value = new Text(w1 + "\t" + count + "\t" + cw1);

				context.write(countKey, countValue);
				context.write(w2Key, w2Value);
			}
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private static Integer w2Sum = 0;
		String curW2, curYear;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keysStrings = key.toString().split("\t");
			String w1, w2, year;

			if (keysStrings[2].equalsIgnoreCase("*")) {
				curYear = keysStrings[0];
				curW2 = keysStrings[1];
				w2Sum = 0;
				for (Text val : values) {
					w2Sum += Integer.parseInt(val.toString());
				}
			} else {
				year = keysStrings[0];
				w2 = keysStrings[1];
				// debug
				if (w2.equalsIgnoreCase(curW2) == false || year.equalsIgnoreCase(curYear) == false) {
					throw new InterruptedException("not matching " + w2 + "," + curW2 + "," + year + "," + curYear);
				}
				for (Text val : values) {
					// Key: year TAB w2 TAB **
					// value: w1 TAB count TAB cw1
					String[] valStrings = val.toString().split("\t");
					Double N = Double.valueOf(context.getConfiguration().get("N_" + year, "0"));
					Double cw1 = 0.0, cw2 = 0.0, cw1w2 = 0.0;
					year = keysStrings[0];
					w1 = valStrings[0];
					w2 = keysStrings[1];
					cw1w2 = Double.valueOf(valStrings[1]);
					cw1 = Double.valueOf(valStrings[2]);
					cw2 = Double.valueOf(w2Sum);

					// calculate
					if (N == 0.0)
						throw new InterruptedException("N is 0.0");

					Double pw1w2, pmi, npmi;

					pw1w2 = cw1w2 / N;

					String debugStr = "year: " + year + " w1: " + w1 + " w2: " + w2 + " cw1w2: " + cw1w2 + " cw1: "
							+ cw1 + " cw2: " + cw2;
					String info;
					if (pw1w2 > Math.pow(2, 50) || pw1w2 < (-1) * Math.pow(2, 50)) {
						info = "pw1w2 too radical: " + pw1w2 + " N: " + N + "\n";
						throw new InterruptedException(info + debugStr);
					}

					if (pw1w2 == 0.0) {
						throw new InterruptedException("pw1w2 is 0.0");
					}

					pmi = Math.log(cw1w2) + Math.log(N) - Math.log(cw1) - Math.log(cw2);

					if (pmi > Math.pow(2, 50) || pmi < (-1) * Math.pow(2, 50)) {
						info = "pmi too radical: " + pmi + "\n";
						throw new InterruptedException(info + debugStr);
					}

					npmi = pmi / (Math.log(pw1w2) * -1);

					if (npmi > Math.pow(2, 50) || npmi < (-1) * Math.pow(2, 50)) {
						info = "npmi too radical: " + npmi + " pmi: " + pmi + " pw1w2: " + pw1w2 + " N: " + N + "\n";
						throw new InterruptedException(info + debugStr);
					}

					// key-value for the N-gram
					Text newKey = new Text(year + "\t" + w1 + "\t" + w2);
					Text newValue = new Text(npmi.toString());

					// increment decade PMI
					npmi = npmi * 100000;
					context.getCounter("pmiCounter", "NPMI_" + year).increment(npmi.longValue());

					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class CWcombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keyString = key.toString().split("\t");
			if (keyString[2].equals("**")) {
				for (Text val : values) {
					context.write(key, val);
				}
			} else {
				Integer countSum = 0;
				for (Text val : values) {
					countSum += Integer.valueOf(val.toString());
				}
				context.write(key, new Text(countSum.toString()));
			}
		}
	}

	public static class CWPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] keyStr = key.toString().split("\t");
			String year = keyStr[0];
			String w = keyStr[1];
			int partitioner = ((year + w).hashCode()) % numReduceTasks;
			if (partitioner < 0) {
				return partitioner * -1;
			}
			return partitioner;
		}
	}

}
