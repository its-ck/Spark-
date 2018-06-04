package spark.innoeye;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class Flatmapdata implements FlatMapFunction {

	@Override
	public Iterator call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}


/*String inputFile = "/home/ist/Downloads/shakespeare.txt";

  JavaRDD < String > input = sc.textFile(inputFile);
  // Split in to list of words
  JavaRDD < String > words = input.flatMap(new Flatmapdata());

  // Transform into pairs and count.
  JavaPairRDD < String, Integer > pairs = words.mapToPair(w -> new Tuple2(w, 1));

  JavaPairRDD < String, Integer > counts = pairs.reduceByKey((x, y) -> x + y);

  System.out.println(counts.collect());*/
