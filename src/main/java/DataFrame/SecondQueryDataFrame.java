package DataFrame;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SecondQueryDataFrame {
    public void executeQuery(SparkSession sparkSession) {
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("assets/BlackFriday.csv");
        csvDataFrame
                .filter(csvDataFrame.col("Purchase").$greater(15000))
                .groupBy("User_ID")
                .count()
                .show(false);
    }
}
