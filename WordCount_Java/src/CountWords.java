
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public final class CountWords { // using MapReduce (sort of)
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public void test() {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		
		SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[1]").set("spark.executor.memory", "2g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = ctx.textFile("C:\\\\Users\\\\sjlof\\\\eclipse-workspace\\WordCount\\test.txt", 1);
		System.out.println("***lines " + lines.count());
		JavaRDD<String> words = lines.flatMap((String s) -> Arrays.asList(SPACE.split(s)).iterator());
		System.out.println("***words " + words.count());
		JavaPairRDD<String, Integer> ones = words.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));
		System.out.println("***ones " + ones.count());
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
		System.out.println("***counts " + counts.count());
		counts.foreach(c -> System.out.println(c._1 + ": " + c._2));
		//Vectors.dense(1.0,2.0,4.0);
		final HashingTF tf = new HashingTF(100);
			
				
		ctx.stop();
		ctx.close();
		

	}
	
	public static void main(String[] args) {
		 CountWords cw = new CountWords();
		 System.out.println("In Main");
		 cw.test();
	}
}