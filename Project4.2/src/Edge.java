import java.io.IOException;
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

public class Edge {


	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("Edge");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = javaSparkContext.textFile("hdfs:///input");
		

		JavaRDD<String> edges = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s);
			}
		});
		JavaPairRDD<String, Integer> ones = edges.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
	
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		int edgeCount = (int) counts.count();
		List<Integer> edgeCountList = Arrays.asList(edgeCount);
		JavaRDD<Integer> output = javaSparkContext.parallelize(edgeCountList);
		output.saveAsTextFile("hdfs:///edgesoutput");
		javaSparkContext.stop();
	}
}