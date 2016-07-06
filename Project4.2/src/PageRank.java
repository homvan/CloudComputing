import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;



public class PageRank {
	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double arg0, Double arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0+arg1;
		}
	}

	public static void main(String[] args) throws IOException {
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = javaSparkContext.textFile("hdfs:///input");
		
		//load data from input file and group by followee
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
	        @Override
	        public Tuple2<String, String> call(String s) {
	          String[] lineParse = s.split(" ");
	          return new Tuple2<String, String>(lineParse[0], lineParse[1]);
	        }
	      }).distinct().groupByKey();
	    
	
	    
		JavaRDD<String> vertices = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String string) {
				String[] lineParse = string.split(" ");
				return Arrays.asList(lineParse);
			}
		});
		//set all node 1.0
		JavaPairRDD<String, Double> ranks = vertices.mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String string) {
				return new Tuple2<String, Double>(string, 1.0);
			}
		}).distinct();
		//get dangling
		JavaPairRDD<String, Double> dangling = vertices.subtract(links.keys()).mapToPair(new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String string) {
				return new Tuple2<String, Double>(string, 1.0);
			}
		}).distinct();
		//calculate rank
		final double verticeNum = vertices.distinct().count();
		
		for (int current = 0; current < 10; current++) {
			
			JavaRDD<Double> danglingNum = ranks.subtractByKey(ranks.subtractByKey(dangling)).values();
		    double danglingSum = danglingNum.flatMap(new FlatMapFunction<Double,Double>(){
				@Override
				public Iterable<Double> call(Double double1){
					return Arrays.asList(double1);
				}
		    }).reduce(new Sum());
		    final double danglingContribs = danglingSum/verticeNum;
		    //calculate node's contributions to the rank of others
		    JavaPairRDD<String, Double> contribs = links.join(ranks).values()
		   	        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
		   	          @Override
		   	          public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
		   	        	int followerNum = 0;
		   	        	for(String follower : s._1){
		   	        		followerNum++;
		   	        	}
		   	            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
		   	            for (String n : s._1) {
		   	              results.add(new Tuple2<String, Double>(n, s._2() / followerNum));
		   	            }
		   	            return results;
		   	          }
		   	      });			
		      
		    
			 JavaRDD<String> nofollowerNodes = vertices.subtract(contribs.keys());
		     JavaPairRDD<String, Double> nofollowerContribs = nofollowerNodes.mapToPair(new PairFunction<String, String, Double>(){
				public Tuple2<String, Double> call(String s) {
					return new Tuple2<String, Double>(s, 0.0);
				}
		     });
		     //calculate  ranks
		     JavaPairRDD<String, Double> allContribs = contribs.union(nofollowerContribs);
		     ranks = allContribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
		         @Override
		         public Double call(Double sum) {
		           return 0.15 + (sum + danglingContribs) * 0.85;
		         }
		       });
		}
		//output format
		JavaRDD<String> output = ranks.flatMap(new FlatMapFunction<Tuple2<String, Double>, String>() {
			public Iterable<String> call(Tuple2<String, Double> t) throws Exception {
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(t._1 + "\t" + t._2.toString());
				return returnValues;
			}
		});
		
		output.saveAsTextFile("hdfs:///pagerankoutput");
		javaSparkContext.stop();
	}
	
}