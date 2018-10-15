package shared;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class Query {
    public abstract void executeQuery(JavaSparkContext sparkContext);

    protected SparkSession buildSession(JavaSparkContext sparkContext) {
        return new SparkSession(sparkContext.sc());
    }
}
