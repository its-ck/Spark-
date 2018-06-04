package spark.innoeye;

import org.apache.spark.sql.api.java.UDF1;

public class Convert implements UDF1<Double, Double> {

	@Override
	public Double call(Double arg0) throws Exception {
		// TODO Auto-generated method stub
		
		return ((arg0 * 9.0 / 5.0) + 32.0);
	}

	
}
