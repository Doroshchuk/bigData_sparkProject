package RDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ThirdQueryRDD {
    public void executeQuery(JavaSparkContext sparkContext) {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("assets/BlackFriday.csv");
        stringJavaRDD
                .map(line -> line.split(","))
                .filter(line -> !line[0].equals("User_ID"))//skip header
                .mapToPair(line -> new Tuple2<>(line[1], 1))//Product_ID column
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(pair ->
                        System.out.println("Product with id " + pair._1 + " is used in " + pair._2 + " orders."));
    }
}
