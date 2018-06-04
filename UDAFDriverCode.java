package spark.innoeye;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UDAFDriverCode {
	
	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession.builder().appName("DataSetUDF").master("local").getOrCreate();
		
		 Dataset<Row> d = spark.read().json("/home/ist/Downloads/Student.json"); 
		 d.createOrReplaceTempView("Student");
		 
		 d.show();
		 spark.udf().register("Student_Data", new DemoUDAF());
		 
		spark.sql("select Student_Data(id,name,marks) from Student where id =1 group by name").show();
	 		 
	}

}
