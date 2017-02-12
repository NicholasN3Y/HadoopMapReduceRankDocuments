package assgn1.cs4225;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RankTopK extends Configured implements Tool {
	
	private static final String INTERMEDIATE_PATH1 = "/assignment_0/intermediate_result/1";
	private static final String INTERMEDIATE_PATH2 = "/assignment_0/intermediate_result/2";
	private static final String INTERMEDIATE_PATH3 = "/assignment_0/intermediate_result/3";
	
	
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
				System.err.println(filename);
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
			line = line.replaceAll("\\p{Punct}", "");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()){
				String token = tokenizer.nextToken().toLowerCase();
				if(!termsToExclude.contains(token)){
					term.set(token+"@"+fileName);
					context.write(term, one);
				}else{
					//System.err.println("Excluded" + token);
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
			System.err.println(key + "\t" +numOfFileWordAppearsIn);
			
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
		
	@Override
	public int run(String[] args) throws Exception {
		
		Path sw = new Path(args[0]);
		Path source = new Path(args[1]);
		Path dest = new Path(args[2]); 
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
		FileOutputFormat.setOutputPath(job3, dest);
		
		return job3.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[]args) throws Exception{
		ToolRunner.run(new Configuration(), new RankTopK(), args);
	}
}
