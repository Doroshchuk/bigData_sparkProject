package SQL;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FirstQuerySQL {
    public void executeQuery(SparkSession sparkSession) {
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("assets/BlackFriday.csv");
        csvDataFrame.createOrReplaceTempView("BlackFriday");
        sparkSession.sql("SELECT * FROM BlackFriday WHERE cast(Marital_Status as int) = 1 " +
                "AND Gender = 'F' " +
                "ORDER BY User_ID")
                .show(false);
    }
}
