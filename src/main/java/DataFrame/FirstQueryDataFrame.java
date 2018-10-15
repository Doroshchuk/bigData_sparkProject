package DataFrame;

import org.apache.spark.sql.*;

import java.util.Arrays;

public class FirstQueryDataFrame {
    public void executeQuery(SparkSession sparkSession) {
        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("assets/BlackFriday.csv");
        csvDataFrame
                .filter(csvDataFrame.col("Marital_Status").equalTo("1"))
                .filter(csvDataFrame.col("Gender").$eq$eq$eq("F"))
                .sort(csvDataFrame.col("User_ID"))
                .show(false);
    }
}
