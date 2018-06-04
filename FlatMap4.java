package spark.innoeye;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FlatMap4 implements PairFunction<String,String,Integer> {

	@Override
	public Tuple2<String, Integer> call(String arg0) throws Exception {
		
		return new Tuple2<String, Integer>(arg0, 1);
	}

}
