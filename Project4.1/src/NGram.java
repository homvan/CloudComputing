import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGram{
	public static class NGram_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static final IntWritable one = new IntWritable(1); 
		private Text Gram = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			if(line.length() == 0){
				return;
			}
			
			line = line.trim().replaceAll("[^a-zA-Z]", " ").toLowerCase();
			String words[] = line.split(" ");
			for(int i=0; i<words.length;i++){
				StringBuilder ngram = new StringBuilder();
				boolean startpoint = true;
				for(int endpoint = 0; endpoint < 5 && endpoint + i < words.length; endpoint ++){
					if(startpoint){
						startpoint = false;
					}
					else{
						ngram.append(" ");
					}
					ngram.append(words[i + endpoint]);
					Gram.set(ngram.toString());
					context.write(Gram, one);
				}
				
			}
			
			
		}
		
		
	}
	
	public static class NGram_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws  IOException, InterruptedException{
			int sum =0;
			for (IntWritable val : values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "NGram");
	    job.setJarByClass(NGram.class);
	    
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(NGram.NGram_Mapper.class);
	    job.setCombinerClass(NGram.NGram_Reducer.class);
	    job.setReducerClass(NGram.NGram_Reducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
	    job.setNumReduceTasks(1);      
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
		
	}
	
	
}