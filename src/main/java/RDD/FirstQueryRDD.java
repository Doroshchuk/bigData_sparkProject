package RDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class FirstQueryRDD {
    public void executeQuery(JavaSparkContext sparkContext) {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("assets/BlackFriday.csv");
        stringJavaRDD
                .map(line -> line.split(","))
                .filter(line -> !line[0].equals("User_ID"))//skip header
                .filter(line -> line[7].equals("1"))//Marital_Status column
                .filter(line -> line[2].equals("F"))//Gender column
                .groupBy(line -> line[0])//User_ID column
                .foreach(line -> {
                    System.out.println(line._1);
                    line._2.forEach(item -> System.out.println("Item: " + Arrays.toString(item)));
                });
    }
}
