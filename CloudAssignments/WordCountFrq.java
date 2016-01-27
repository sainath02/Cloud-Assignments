import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

//more explanation of important steps is given in ReadMe.pdf 

public class WordCountFrq {

	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

		static enum CountersEnum { INPUT_WORDS }
	
		private Text word = new Text();
	
		private boolean caseSensitive;
		private Set<String> matchingPattern = new HashSet<String>();
	
		private Configuration conf;
		private BufferedReader fis;
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
			
			if (conf.getBoolean("wordcount.add.patterns", true)) {
				URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
				for (URI patternsURI : patternsURIs) {
					Path patternsPath = new Path(patternsURI.getPath());
					String patternsFileName = patternsPath.getName().toString();
					parseMatchFile(patternsFileName);
				}
			}
		}
	
		private void parseMatchFile(String fileName) {
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				String[] words = null;
			
				while ((pattern = fis.readLine()) != null) {
					words = pattern.split(" ");
					for ( String ss : words){
						matchingPattern.add(ss);
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"
				+ StringUtils.stringifyException(ioe));
		}
		}
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = (caseSensitive) ?
				value.toString() : value.toString().toLowerCase();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String cur = itr.nextToken();
				if(matchingPattern.contains(cur)){
					word.set(cur);
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer
		extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		
		if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
			System.err.println("Usage: wordcount <in> <out> [-add PatternMatchFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountFrq.class);
    
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i=0; i < remainingArgs.length; ++i) {
			if ("-add".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.add.patterns", true);
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}
		
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		JobClient.runJob(conf);
	}
}
