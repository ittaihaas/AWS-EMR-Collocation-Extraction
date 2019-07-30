package com.amazonaws.dsps192;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Step0 {

	public static class Mapper0 extends Mapper<Object, Text, Text, Text> {
		public String[] stopWords = { "a", "about", "above", "across", "after", "afterwards", "again", "against", "all",
				"almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst",
				"amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway",
				"anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes",
				"becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between",
				"beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer",
				"con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during",
				"each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
				"ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
				"find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
				"full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
				"hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
				"however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
				"itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
				"meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
				"my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
				"none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
				"only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
				"part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
				"seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty",
				"so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such",
				"system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence",
				"there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
				"third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together",
				"too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon",
				"us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
				"where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which",
				"while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
				"without", "would", "yet", "you", "your", "yours", "yourself", "yourselves" };

		private String w1, w2, count;
		private int year;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> stopWordsMap = new HashMap<String, Integer>();
			for (String word : stopWords)
				stopWordsMap.put(word.toLowerCase(), 1);
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

			while (itr.hasMoreTokens()) {
				String[] ngramEntry = itr.nextToken().split("\t");
				// ngramEntry structure:
				// W1 SPACE W2 TAB 1973 TAB count TAB pages TAB books
				try {
					w1 = ngramEntry[0].split(" ")[0];
					w2 = ngramEntry[0].split(" ")[1];
					year = Integer.parseInt(ngramEntry[1]);
					year = (year / 10) * 10;
					count = ngramEntry[2];
					// remove stop words
					if (stopWordsMap.containsKey(w1.toLowerCase()) || stopWordsMap.containsKey(w2.toLowerCase()))
						continue;
				} catch (IndexOutOfBoundsException e) {
					System.out.println(e.getMessage());
					System.out.println("@@Mapper0 wrong entry structure: " + Arrays.deepToString(ngramEntry));
					continue;
				}

				// new entry structure:
				// KEY: year TAB w1 TAB w2
				// VALUE: count
				Text newEntry = new Text(Integer.toString(year) + "\t" + w1 + "\t" + w2);
				context.write(newEntry, new Text(count));
			}
		}
	}

	public static class Reducer0 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// entry structure:
			// KEY: year TAB w1 TAB w2
			// VALUE: count
			int sum = 0;
			for (Text val : values) {
				sum += Integer.valueOf(val.toString());
			}
			context.write(key, new Text(String.valueOf(sum)));
		}
	}
}
