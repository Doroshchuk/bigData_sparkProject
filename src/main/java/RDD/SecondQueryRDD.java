package RDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SecondQueryRDD {
    public void executeQuery(JavaSparkContext sparkContext) {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("assets/BlackFriday.csv");
        stringJavaRDD
                .map(line -> line.split(","))
                .filter(line -> !line[0].equals("User_ID"))//skip header
                .filter(line -> Integer.parseInt(line[11]) > 15000)//Purchase header
                .mapToPair(line -> new Tuple2<>(line[0], 1))//User_ID column
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(pair ->
                        System.out.println("User with id " + pair._1 + " has " + pair._2 + " orders with purchase more then 15000."));
    }
}
