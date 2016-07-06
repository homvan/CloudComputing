import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class Follower {

	
	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("Follower");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = javaSparkContext.textFile("hdfs:///input");
		
	
		JavaRDD<String> edges = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s);
			}
		});
		//get followees
		JavaPairRDD<String, Integer> followees = edges.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] tmp = s.split(" ");
				String followee = tmp[1];
				return new Tuple2<String, Integer>(followee, 1);
			}
		});
		//get follower in case of one has no follower
		JavaPairRDD<String, Integer> followers = edges.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] tmp = s.split(" ");
				String follower = tmp[0];
				return new Tuple2<String, Integer>(follower, 0);
			}
		});
		//put together
		JavaPairRDD<String, Integer> tmpresult = followees.union(followers);
		
		JavaPairRDD<String, Integer>followerresult = tmpresult.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		//output format
		JavaRDD<String> followoutput = followerresult.flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {
			public Iterable<String> call(Tuple2<String, Integer> t) throws Exception {
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(t._1 + "\t" + t._2.toString());
				return returnValues;
			}
		});
		
		followoutput.saveAsTextFile("hdfs:///followeroutput");
		javaSparkContext.stop();
		
	}
}