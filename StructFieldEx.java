package spark.innoeye;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructFieldEx {
	
	public static void main(String[] args) {
		
 SparkSession spark = SparkSession.builder().appName("DataSetUDF").master("local").getOrCreate();
		 
		 Dataset<Row> d = spark.read().text("/home/ist/Downloads/Student.txt"); 
		
		 
		String schema = "id name marks";
		List<StructField> sf = new ArrayList<>();
		
		for(String fName : schema.split(" "))
		{
		StructField sfld0 = DataTypes.createStructField(fName, DataTypes.StringType, true);
		sf.add(sfld0);
	}
		StructType sch = DataTypes.createStructType(sf);
		Dataset<Row> dsrow = spark.createDataFrame(d.toJavaRDD(), sch);
		
		dsrow.createOrReplaceTempView("Student_Data");
		//dsrow.show();
		
		Dataset<Row> res = spark.sql("Select * from Student_Data");
		res.show();
	}

}
