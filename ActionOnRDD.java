package spark.innoeye;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mortbay.util.Scanner;

public class ActionOnRDD {
	
	public static void main(String[] args) {
		
		
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("new");
		  JavaSparkContext sc = new JavaSparkContext(conf);
		  
		JavaRDD<Integer> no = sc.parallelize(Arrays.asList(5, 6, 7,8,9, 10, 11,12));
		
		List<Integer> ls = no.collect();
		List<Integer> ls1 = no.top(2);
		List<Integer> ls2 = no.take(4);
		long l = no.count();
		
		System.out.println(ls.toString());
		System.out.println(ls1.toString());
		System.out.println(ls2.toString());
		 System.out.println(l);
		 
		 long red = no.reduce((n1,n2) ->n1+n2);
		 
		 System.out.println(red);
		
		
		 
		 sc.close();
	}

}
