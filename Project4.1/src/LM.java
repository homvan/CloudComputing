import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LM{
	public static class LM_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text output_prefix = new Text();
		private Text output_suffix = new Text();
		
		private int threshold = 2;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineparse = line.split("\t");
			String[] words = lineparse[0].split(" ");
			String rowkey = words[0];
			String word = null;
			int count = Integer.parseInt(lineparse[1]);
			if(count < threshold){
				return;
			}
			
			//check gram's length, if <= 1, ignore it
			if(words.length > 1){
				for (int i=1;i<words.length-1;i++){
					rowkey = rowkey+" "+words[i];
				}
				word = words[words.length-1];
			}
			
			
			output_prefix.set(rowkey);
			output_suffix.set(word + ":" +count);
			context.write(output_prefix, output_suffix);
		}
	}
	
	public static class LM_Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//map to sort
			int sum = 0;
			Map <String, Integer> map = new HashMap<String, Integer>();
			for (Text value:values){
				String[] valueparse = value.toString().split(":");
				String word = valueparse[0];
				int count = Integer.parseInt(valueparse[1]);
				map.put(word, count);
				sum += count;
			}
			// sort map
			List<Map.Entry<String, Integer>> sortlist = new ArrayList<Map.Entry<String, Integer>> (map.entrySet());
            Collections.sort(sortlist,
                new Comparator <Map.Entry<String, Integer>>() {
                    public int compare(Map.Entry < String, Integer > o1,
                        Map.Entry < String, Integer > o2) {
                       	int res = o1.getValue().compareTo(o2.getValue());
                        if (res == 0){
                        	res = o1.getKey().compareTo(o2.getKey());
                        }
                        return res;
                    }
                }); 
            //put into hbase
            Put put = new Put(Bytes.toBytes(key.toString()));
            for (int i=0;i<5 && i<map.size(); i++){
            	String cur_word = sortlist.get(i).getKey();
                int cur_count = sortlist.get(i).getValue();
                double Probability = cur_count * 100.0 / sum;
           
                put.add(Bytes.toBytes("Probability"), Bytes.toBytes(cur_word), Bytes.toBytes("" + Probability));
            }
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new HBaseConfiguration();
	    Job job = Job.getInstance(conf, "LM");
	    job.setJarByClass(LM.class);
	    job.setMapperClass(LM.LM_Mapper.class);
	    job.setReducerClass(LM.LM_Reducer.class);
	    TableMapReduceUtil.initTableReducerJob("LM", LM_Reducer.class, job);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.waitForCompletion(true);
	}
	
	
}

