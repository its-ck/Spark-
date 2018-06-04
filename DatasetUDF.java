package spark.innoeye;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.sql.types.DataTypes;

public class DatasetUDF {
	
	
	public static void main(String[] args) {
		
		 SparkSession spark = SparkSession.builder().appName("DataSetUDF").master("local").getOrCreate();
		 
		 /*Dataset<String> td = spark.createDataset(Arrays.asList("Chandan", "Hemanth"), Encoders.STRING());
		 
		 td.show();*/
		 
		 Dataset<Row> d = spark.read().json("/home/ist/Downloads/temp.json"); 
		 d.show();
		
		 d.createOrReplaceTempView("Temperature");
		
		 //Writing UDF
			 spark.udf().register("DemoUDAF", new Convert(), DataTypes.DoubleType);
			 
			 spark.sql("select city, convertData(avgLow) AS avgTemp from Temperature").show();
		
			
		/*	
		  spark.udf().register("CEL_TO_FAHT", new UDF1<Double, Double>()
		 {  public Double call(Double degreesCelcius) {
			    return ((degreesCelcius * 9.0 / 5.0) + 32.0); }
			}, DataTypes.DoubleType);
		 
		 spark.sql("SELECT city, CEL_TO_FAHT(avgLow) AS avgLF, CEL_TO_FAHT(avgHigh) AS avgHF FROM Temperature").show();
			spark.sql("Select city from Temperature").show();
			
			spark.sql("Select MIN(avgLow) as Lowest_Temperature from Temperature").show();
			
			spark.sql("Select MAX(avgHigh) as Highest_Temperature from Temperature").show();
			
			spark.sql("select SUM(avgLow) from Temperature").show();
			
			spark.sql("select SUM(avgHigh) from Temperature").show();
			
			spark.sql("select AVG(avgLow) from Temperature").show();
			
			spark.sql("select AVG(avgHigh) from Temperature").show();*/
			
	}

}
