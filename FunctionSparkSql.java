package spark.innoeye;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.col;

public class FunctionSparkSql {
	
	public static void main(String[] args) throws AnalysisException {
		 SparkSession spark = SparkSession.builder().appName("DataSetApp").master("local").getOrCreate();
		 
		 Dataset<Person> people = spark.read().json("/home/ist/Downloads/data.json").as(Encoders.bean(Person.class));
		 people.createOrReplaceTempView("Student"); 
		 
		 Dataset<Row> sql1 = spark.sql("select * from Student");
		// Dataset<Row> sql1 = spark.sql("select * from Student where id =1");
		  sql1.show(false);
		/*  long count = sql1.count();
		  
		  System.out.println("No of Rows :" +count);
		  
		  List<Row> col_ls = sql1.collectAsList();
		  for (Iterator itr = col_ls.iterator(); itr.hasNext();) {
			Row row = (Row) itr.next();
			
			System.out.println(row);
			
		}
		  
		  Dataset<Row> des = sql1.describe();
		  des.show(false);
		  
		  Dataset<Row> dst = sql1.distinct();
		  dst.show(false);
		  
		  Dataset<Row> filter = sql1.filter("gender = Male");
		  filter.show();
		  
		  Dataset<Row> drop = sql1.drop("email");
		  drop.show();
		  
		  Dataset<Row> dd = sql1.dropDuplicates();
		  dd.show();
		  
		  Dataset<Row> coalesce = sql1.coalesce(5);
		  coalesce.show();
		  
		  sql1.explain();
		  
		  
		  ExpressionEncoder<Row> exprEnc = sql1.exprEnc();
		  exprEnc.show();
		  
		  
		  Row first = sql1.first();
		  System.out.println("First Row is : " +first);
		  
		  NamedExpression resolve = sql1.resolve("lname");
		  System.out.println(resolve);
		
		  sql1.write().json("/home/ist/Downloads/new.json");
		  
		  sql1.select("fname" , "lname").where("id = 2");
		  sql1.show();
		  
		  WindowSpec rowsBetween = Window.partitionBy("fname").orderBy("id").rowsBetween(-1, 1);
		  
		  System.out.println("////////");
		  System.out.println(rowsBetween);
		 
		 System.out.println(rowsBetween.toString()); 
		 
		 StructType schema = sql1.schema();
		System.out.println(schema);
		  */
		  people.createOrReplaceTempView("Student"); 
//		  people.select(functions.col("lname")).show();
		  
		  people.withColumn("aravind", functions.col("lname")).show();
		  
		 /* error
		  * Dataset<Row> ds1 = spark.sql("select col(lname) from Student");
		 ds1.show();
		 
		 sql1.select(col("lname"), col("id"));
		 sql1.show();*/
		 
		  NamedExpression resolve = sql1.resolve("lname");
		  
		  System.out.println(resolve.qualifiedName());
		
		  
		  
	}
}