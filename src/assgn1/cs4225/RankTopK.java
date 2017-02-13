package assgn1.cs4225;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RankTopK extends Configured implements Tool {

	private static final String INTERMEDIATE_PATH1 = "a0112224/assignment_0/intermediate_result/1";
	private static final String INTERMEDIATE_PATH2 = "a0112224/assignment_0/intermediate_result/2";
	private static final String INTERMEDIATE_PATH3 = "a0112224/assignment_0/intermediate_result/3";
	private static final String INTERMEDIATE_PATH4 = "a0112224/assignment_0/intermediate_result/4";


	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text term = new Text();

		private Set<String> termsToExclude = new HashSet<String>();

		private Configuration config;
		private BufferedReader fis;


		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			config = context.getConfiguration();
			URI[] skipfiles = Job.getInstance(config).getCacheFiles();
			for (URI skipfileURI: skipfiles){
				Path skipfilePath = new Path(skipfileURI.getPath());
				String skipfileName = skipfilePath.getName().toString();
				parseSkipFile(skipfileName);
			}
		}

		private void parseSkipFile(String filename){
			try{

				fis = new BufferedReader(new FileReader(filename));
				String term = null;
				while((term = fis.readLine()) != null) {
					termsToExclude.add(term);
				}
			} catch (IOException e){
				System.err.println("Exception caught while parsing");
			}
		}

		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException { 
			//get the name of file containing this key/value pair
			//read one line, tokenize into  (word@filename, 1) pairs	
			String fileName=((FileSplit)context.getInputSplit()).getPath().getName();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()){
				String token_orig = tokenizer.nextToken().toLowerCase();
				String [] tokens = token_orig.split("--");
				for (String token : tokens ){
					token = token.replaceFirst("^[^a-zA-Z]+", "");
					token = token.replaceAll("[^a-zA-Z]+$", "");
					token = token.trim();

					if (token.equals("")){
						continue;
					}
					if(!termsToExclude.contains(token)){
						System.out.println(token);
						term.set(token+"@"+fileName);
						context.write(term, one);
					}else{
						//System.err.println("Excluded" + token);
					}
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws 	IOException, InterruptedException { 
			//sum up all the values, output (word@filename,freq) pair
			int freq = 0;
			for(IntWritable val : values){
				freq += val.get();
			}
			context.write(key, new IntWritable(freq));
		} 		
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//parse the key/value pair into word, filename, frequency
			Text output_key = new Text();
			Text output_value = new Text();
			String line = value.toString();
			String [] terms = line.split("\\t");
			String [] word_filename = terms[0].split("@");
			assert(terms.length == 2);
			assert (word_filename.length == 2);
			int freq = Integer.parseInt(terms[1]);
			String term = word_filename[0];
			String filename = word_filename[1];
			//check if filename is unique 
			output_key.set(term);
			output_value.set(filename + "=" + freq);
			//output a pair (word, filename=frequency
			//System.out.println(term + " in " + filename + " has " + freq);
			context.write(output_key, output_value);
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int numOfFileWordAppearsIn = 0;
			Iterator<Text> itr = values.iterator();
			Map<String, Integer>tempHashset = new HashMap<>();
			while(itr.hasNext()){
				String[] value_sep = itr.next().toString().split("=");
				tempHashset.put(value_sep[0], Integer.parseInt(value_sep[1]));
				numOfFileWordAppearsIn++;
			}
			System.out.println(key + "\t" +numOfFileWordAppearsIn);

			int numOfDocs = Integer.parseInt(context.getConfiguration().get("numOfDocs"));

			Text output_key = new Text();
			DoubleWritable output_value = new DoubleWritable();
			for (String filename : tempHashset.keySet()){
				int freq = tempHashset.get(filename);
				System.err.println("Number of documents is " + numOfDocs + " frequency "+ freq);
				double tfidf = (1 + Math.log(freq)) * (double)(Math.log((double)numOfDocs / numOfFileWordAppearsIn));
				System.err.println("2nd Job "+key+"@"+filename+" with "+tfidf + " appears in " + numOfFileWordAppearsIn + " documents");
				output_key.set(key+"@"+filename);
				output_value.set(tfidf);
				context.write(output_key, output_value);
			}
		}
	}

	public static class Mapper3 extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//parse the key/value pair into word, filename, tfidf
			//output a pair(filename, word=tfidf)
			Text output_key = new Text();
			Text output_value = new Text();
			String line = value.toString();
			String [] terms = line.split("\\t");
			String [] word_filename = terms[0].split("@");
			assert(terms.length == 2);
			assert (word_filename.length == 2);                   
			output_key.set(word_filename[1]);
			output_value.set(word_filename[0]+"="+terms[1]);
			System.out.println(output_key+" "+output_value);
			context.write(output_key, output_value);
		}
	}	

	public static class Reducer3 extends Reducer<Text, Text, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//key is a filename, values are in the form of (word=tfidf)
			//for every word-tfidf in the values, output(word@filename, norm-tfidf)
			Text output_key = new Text();
			DoubleWritable output_value = new DoubleWritable();
			Map<String, Double> tempHashSet = new HashMap<String, Double>();
			//store list of all tfidf values
			for(Text value : values){
				String [] temp = value.toString().split("=");
				String word = temp[0];
				Double tfidf = Double.parseDouble(temp[1]);
				tempHashSet.put(word, tfidf);
			}

			double squaredSumOfTfIdf = 0;
			for (Double hashValue : tempHashSet.values()){
				squaredSumOfTfIdf += Math.pow(hashValue, 2);
			}
			System.out.println(squaredSumOfTfIdf);
			for (String term : tempHashSet.keySet()){
				double tfidf = tempHashSet.get(term);
				double norm_tfidf = tfidf / Math.sqrt(squaredSumOfTfIdf);
				output_key.set(term+"@"+key);
				output_value.set(norm_tfidf);
				context.write(output_key, output_value);
			}		
		}
	}

	public static class Mapper4 extends Mapper<Object, Text, Text, Text>{

		private Map<String, Integer> queryTerms = new HashMap<String, Integer>();
		private Configuration config;
		private BufferedReader fis;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			config = context.getConfiguration();
			URI[] queryfilePaths = Job.getInstance(config).getCacheFiles();
			for (URI queryfileURI: queryfilePaths){
				Path queryfile = new Path(queryfileURI.getPath());
				String query = queryfile.getName().toString();
				parseSkipFile(query);
			}
		}

		private void parseSkipFile(String filename){
			try{

				fis = new BufferedReader(new FileReader(filename));
				String term = null;
				while((term = fis.readLine()) != null) {
					if (queryTerms.containsKey(term)){
						int occurrence = queryTerms.get(term).intValue();
						queryTerms.put(term, occurrence++);
					}else{
						queryTerms.put(term,1);
					}
				}
			} catch (IOException e){
				System.err.println("Exception caught while parsing query file");
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//parse the key/value pair into word, filename, norm-tfidf
			//if the word is contained in the query file, output (filename, word=norm-tfidf)
			Text output_key = new Text();
			Text output_value = new Text();
			String line = value.toString();
			String [] terms = line.split("\\t");
			String [] word_filename = terms[0].split("@");
			String word = word_filename[0];
			String doc = word_filename[1];
			double norm_tfidf = Double.parseDouble(terms[1]);
			if (queryTerms.containsKey(word)){
				output_key.set(doc);
				output_value.set(word+"="+norm_tfidf);
				int occurrence = queryTerms.get(word);
				while (occurrence != 0){
					System.out.println(output_key + " " + output_value);
					context.write(output_key, output_value);
					occurrence--;
				}
			}
		}	
	}

	public static class Reducer4 extends Reducer<Text, Text, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			//Note: key is a filename, values are in the form of (word=norm-tfidf)
			//sum up all the norm-tfidfs in the values, output (filename,total-norm-tfidf) pair\
			Text output_key = new Text();
			DoubleWritable output_value = new DoubleWritable();
			double total = 0;
			for(Text val : values){
				double norm_tfidf = Double.parseDouble(val.toString().split("=")[1]);
				total += norm_tfidf;
			}
			if (total != 0){
				output_key.set(key);
				output_value.set(total);
				context.write(output_key, output_value);
			}
		} 		
	}
	
	public static class Mapper5 extends Mapper<Object, Text, DoubleWritable, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			DoubleWritable output_key = new DoubleWritable();
			Text output_value = new Text();
			String [] temp = value.toString().split("\\t");
			output_key.set(Double.parseDouble(temp[1]));
			output_value.set(temp[0]);
			context.write(output_key, output_value);
		}
	}
	
	public static class Reducer5 extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			DoubleWritable output_key = new DoubleWritable();
			Text output_value = new Text();
			for (Text filename : values){
				output_key.set(key.get());
				output_value.set(filename);
				context.write(output_key, output_value);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class KeyDescendingComparator extends WritableComparator {
		protected KeyDescendingComparator(){
			super(DoubleWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			DoubleWritable key1 = (DoubleWritable) o1;
			DoubleWritable key2 = (DoubleWritable) o2;
			
			if (key1.get() < key2.get()){
				return 1;
			}else if(key1.get() == key2.get()){
				return 0;
			}else{
				return -1;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {


		Path source = new Path(args[0]);
		Path dest = new Path(args[1]); 
		Path sw = new Path(args[2]);
		Path query = new Path(args[3]);

		FileSystem fs = FileSystem.get(getConf());
		ContentSummary cs = fs.getContentSummary(source);
		long filecount = cs.getFileCount();

		/*
		 * Job 1 : Map1 -> Reduce1
		 */

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DocumentTokenizer");
		job.setJarByClass(RankTopK.class);

		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.addCacheFile(sw.toUri());

		FileInputFormat.addInputPath(job, source);
		FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_PATH1));

		job.waitForCompletion(true);

		/*
		 * Job 2 : Map2 -> Reduce2		
		 */
		Configuration conf2 = new Configuration();

		conf2.set("numOfDocs", ""+filecount);
		Job job2 =  Job.getInstance(conf2, "TfIdfProducer");
		job2.setJarByClass(RankTopK.class);

		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_PATH1));
		FileOutputFormat.setOutputPath(job2, new Path(INTERMEDIATE_PATH2));

		job2.waitForCompletion(true);

		/*
		 * Job 3 : Map3 -> Reduce3
		 */
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "TfIdfNormalizer");
		job3.setJarByClass(RankTopK.class);

		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(INTERMEDIATE_PATH2));
		FileOutputFormat.setOutputPath(job3, new Path(INTERMEDIATE_PATH3));

		job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "QuerySolver");
		job4.setJarByClass(RankTopK.class);

		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.addCacheFile(query.toUri());

		FileInputFormat.addInputPath(job4, new Path(INTERMEDIATE_PATH3));
		FileOutputFormat.setOutputPath(job4, new Path(INTERMEDIATE_PATH4));
		
		job4.waitForCompletion(true);

		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5, "ResultSorter");
		job5.setJarByClass(RankTopK.class);

		job5.setMapperClass(Mapper5.class);
		job5.setSortComparatorClass(KeyDescendingComparator.class);
		job5.setReducerClass(Reducer5.class);
		job5.setOutputKeyClass(DoubleWritable.class);
		job5.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job5, new Path(INTERMEDIATE_PATH4));
		FileOutputFormat.setOutputPath(job5, dest);
		
		return job5.waitForCompletion(true)? 0 : 1;
		
	}

	public static void main(String[]args) throws Exception{
		ToolRunner.run(new Configuration(), new RankTopK(), args);
		try{
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1]+Path.SEPARATOR+"part-r-00000"))));
			String term = null;
			int K = 5;
		
			if (args.length == 5){
				K = Integer.parseInt(args[4]);
			}
			
			System.out.println("\n\n\n****** Results ******");
			System.out.println("Top " + K + "Documents corresponding to query is by order:");
			while((term = fis.readLine()) != null && K > 0) {
				System.out.println(term.split("\\t")[1]);
				K--;
			}
			fis.close();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		System.out.println("Here is the answer to everything!");
	}

}
