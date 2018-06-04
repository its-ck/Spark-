package spark.innoeye;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetEx {
	
	public static void main(String[] args) throws AnalysisException {
		 SparkSession spark = SparkSession.builder().appName("DataSetApp").master("local").getOrCreate();
		 
		 Dataset<Person> people = spark.read().json("/home/ist/Downloads/data.json").as(Encoders.bean(Person.class));
		 
		 people.show();
		/* people.createTempView("temp");
		 people.sqlContext().sql("select email from temp");
		 */
		 people.printSchema();
		 
		/*Dataset<Row> fDS = people.select("fname" , "id");
		  fDS.show();*/
		 
		  people.registerTempTable("Student");
		  Dataset<Row> sql = spark.sql("select * from Student where id=2");
		  sql.show();
		  
		  Dataset<Row> sql2 = spark.sql("select email from Student where id=34");
		  sql2.show();
		  
		  Dataset<Row> sql3 = spark.sql("select fname , lname from Student where id BETWEEN 10 AND 100");
		  sql3.show(150,false);
		  
		  
		/*For json file encoder is not needed as json files already have schema , 
		  encoders provide Schema according to wrapper.
		
		  
		  Dataset<Row> d = spark.read().json("/home/ist/Downloads/data.json"); 
		 
		 d.show();*/
		 spark.stop();
		    
	}
	
	}


