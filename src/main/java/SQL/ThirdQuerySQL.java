package SQL;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ThirdQuerySQL {
    public void executeQuery(SparkSession sparkSession) {
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("assets/BlackFriday.csv");
        csvDataFrame.createOrReplaceTempView("BlackFriday");
        sparkSession.sql("SELECT Product_ID, count(*) FROM BlackFriday GROUP BY Product_ID")
                .show(false);
    }
}
