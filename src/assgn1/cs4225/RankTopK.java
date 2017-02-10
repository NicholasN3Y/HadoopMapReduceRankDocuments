package assgn1.cs4225;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RankTopK {
	
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
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "TopKRanker");
		job.setJarByClass(RankTopK.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.addCacheFile(new Path(args[0]).toUri());
		
		Path source = new Path(args[1]);
		Path dest = new Path(args[2]); 
		FileInputFormat.addInputPath(job, source);
		FileOutputFormat.setOutputPath(job, dest);
		
		
//		// TODO: specify input and output DIRECTORIES (not files)

		if (!job.waitForCompletion(true))
			return;
	}

}
