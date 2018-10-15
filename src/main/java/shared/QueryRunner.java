package shared;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class QueryRunner {
    protected JavaSparkContext context;
    protected SparkSession sparkSession;

    public QueryRunner(JavaSparkContext context) {
        this.context = context;
        this.sparkSession = new SparkSession(context.sc());
    }
    public abstract void executeFirstQuery();
    public abstract void executeSecondQuery();
    public abstract void executeThirdQuery();
}
