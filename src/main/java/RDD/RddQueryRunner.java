package RDD;

import DataFrame.FirstQueryDataFrame;
import DataFrame.SecondQueryDataFrame;
import DataFrame.ThirdQueryDataFrame;
import org.apache.spark.api.java.JavaSparkContext;
import shared.QueryRunner;

public class RddQueryRunner extends QueryRunner {
    public RddQueryRunner(JavaSparkContext context) {
        super(context);
    }

    public void executeFirstQuery() {
        FirstQueryRDD query = new FirstQueryRDD();
        query.executeQuery(context);
    }
    public void executeSecondQuery() {
        SecondQueryRDD query = new SecondQueryRDD();
        query.executeQuery(context);
    }
    public void executeThirdQuery() {
        ThirdQueryRDD query = new ThirdQueryRDD();
        query.executeQuery(context);
    }

}
