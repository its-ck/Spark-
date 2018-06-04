package spark.innoeye;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;

public class WordCount {
 public static void main(String[] args) {

  SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
  JavaSparkContext sc = new JavaSparkContext(conf);

  String inputFile = "/home/ist/Downloads/shakespeare.txt";
  JavaRDD<String> rdd = sc.textFile(inputFile);
  
  JavaRDD<String> flatMap = rdd.flatMap(new FlatMap1());
  
  	JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new FlatMap4());
  	
  		JavaPairRDD<String,Integer> reduceByKey = mapToPair.reduceByKey(new FlatMap6());
  		
  	
  		for(Tuple2<String, Integer> t1:reduceByKey.collect()) {
  			System.out.println("key => "+t1._1);
  			System.out.println(" value = "+t1._2);
  		}
  		
  		for(Tuple2<String, Integer> t2:reduceByKey.take(3)) {
  			System.out.println("key => "+t2._1);
  			System.out.println(" value = "+t2._2);
  		}
  		
  		
  		
  	
 }
}
