package spark.innoeye;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMap1 implements FlatMapFunction<String, String> {

	@Override
	public Iterator call(String arg0) throws Exception {
		
		String [] s = arg0.split(" ");
		return Arrays.asList(s).iterator();
		 
	}

}
