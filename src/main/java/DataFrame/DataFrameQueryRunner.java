package DataFrame;

import org.apache.spark.api.java.JavaSparkContext;
import shared.QueryRunner;

public class DataFrameQueryRunner extends QueryRunner {
    public DataFrameQueryRunner(JavaSparkContext context) {
        super(context);
    }

    public void executeFirstQuery() {
        FirstQueryDataFrame firstQueryDataframe = new FirstQueryDataFrame();
        firstQueryDataframe.executeQuery(sparkSession);
    }
    public void executeSecondQuery() {
        SecondQueryDataFrame secondQueryDataframe = new SecondQueryDataFrame();
        secondQueryDataframe.executeQuery(sparkSession);
    }
    public void executeThirdQuery() {
        ThirdQueryDataFrame thirdQueryDataframe = new ThirdQueryDataFrame();
        thirdQueryDataframe.executeQuery(sparkSession);
    }

}
