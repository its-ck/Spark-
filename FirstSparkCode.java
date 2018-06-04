package spark.innoeye;

import java.util.Arrays;
import org.apache.spark.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class FirstSparkCode
{		
		public static void main(String[] args) {
	    if (args.length < 2) {
	      System.err.println("Please provide the full path of input file and output dir as arguments");
	      System.exit(0);
	    }

	    
	    SparkSession spark = SparkSession.builder().master("local").appName("WordCount").getOrCreate();
	    
	    
	    Dataset<String> df = spark.read().text(args[0]).as(Encoders.STRING());
	    Dataset<String> words = df.flatMap(s -> {
	    								return  Arrays.asList(s.toLowerCase().split(" ")).iterator(); 
	    							}, Encoders.STRING())
	    							.filter(s -> !s.isEmpty())
	    							.coalesce(1); //one partition (parallelism level)
	 //   words.printSchema();  
	   //  { value: string (nullable = true)
	    //}
	    Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
	    		.count()
	    		.toDF("word","count");
	    t = t.sort(functions.desc("count"));
	    t.toJavaRDD().saveAsTextFile(args[1]);
	  }
	
			
	}
	
