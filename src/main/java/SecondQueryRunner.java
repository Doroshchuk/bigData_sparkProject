import DataFrame.FirstQueryDataFrame;
import DataFrame.SecondQueryDataFrame;
import RDD.FirstQueryRDD;
import RDD.SecondQueryRDD;
import SQL.FirstQuerySQL;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class SecondQueryRunner {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        SparkSession sparkSession = new SparkSession(sparkContext.sc());

        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end;

        long rddQueryExecutionTime;
        long dataFrameQueryExecutionTime;
        long sqlQueryExecutionTime;

        //First runQuery RDD
        SecondQueryRDD firstQueryRDD = new SecondQueryRDD();
        firstQueryRDD.executeQuery(sparkContext);

        end = LocalDateTime.now();
        rddQueryExecutionTime = ChronoUnit.MILLIS.between(start, end);

        //First query dataframe
        start = LocalDateTime.now();
        SecondQueryDataFrame firstQueryDataframe = new SecondQueryDataFrame();
        firstQueryDataframe.executeQuery(sparkSession);
        end = LocalDateTime.now();
        dataFrameQueryExecutionTime = ChronoUnit.MILLIS.between(start, end);

        //First query SQL
        start = LocalDateTime.now();
        FirstQuerySQL firstQuerySQL = new FirstQuerySQL();
        firstQuerySQL.executeQuery(sparkSession);
        end = LocalDateTime.now();

        sqlQueryExecutionTime = ChronoUnit.MILLIS.between(start, end);

        System.out.println("RDD query execution is " + rddQueryExecutionTime + " millis");
        System.out.println("DataFrame query execution is " + dataFrameQueryExecutionTime + " millis");
        System.out.println("SQL query execution is " + sqlQueryExecutionTime + " millis");
    }
}
