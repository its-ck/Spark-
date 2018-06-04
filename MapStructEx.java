package spark.innoeye;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MapStructEx {
	
	public static void main(String[] args) throws AnalysisException {
		 SparkSession spark = SparkSession.builder().appName("DataSetApp").master("local").getOrCreate();
		 
		 Dataset<Person> people = spark.read().json("/home/ist/Downloads/data.json").as(Encoders.bean(Person.class));
		 people.createOrReplaceTempView("Student"); 
		 
		 /* Dataset<Row> sql1 = spark.sql("select  struct(fname , lname) as struct from Student where id BETWEEN 10 AND 100");
		  sql1.show(150,false);
		  sql1.printSchema();
		  
		  Dataset<Row> sql3 = spark.sql("select  array(gender, lname) as array from Student where id = 555");
		  sql3.show(150,false);
		  sql3.printSchema();
		  
		  
		  //Exploding array
		  Dataset<Row> sql4 = spark.sql("select  array(fname,lname) as fullname from Student");
		  sql4.show(150,false);
		  sql4.printSchema();
		  
		 sql4.createOrReplaceTempView("testData");
		  Dataset<Row> pers =spark.sql("select *, explode(fullname) as Split_Data from testData");
		  pers.show();
		  pers.printSchema(); */
		  
		 
		 //Exploding Array
		  Dataset<Row> sql2 = spark.sql("select  map(fname,lname,id,email) as map_Data from Student");
		  sql2.show(false);
		  		  
		  sql2.createOrReplaceTempView("newtestData");
		  Dataset<Row> pers =spark.sql("select *, explode(map_Data) from newtestData");
		  pers.show();
		  pers.printSchema();
		  
		  
		 spark.stop();
		    
	}
	
	}


