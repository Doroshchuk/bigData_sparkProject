package SQL;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SecondQuerySQL {
    public void executeQuery(SparkSession sparkSession) {
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("assets/BlackFriday.csv");
        csvDataFrame.createOrReplaceTempView("BlackFriday");
        sparkSession.sql("SELECT User_ID, count(*) FROM BlackFriday WHERE cast(Purchase as int) > 15000 " +
                "GROUP BY User_ID")
                .show(false);
    }
}
