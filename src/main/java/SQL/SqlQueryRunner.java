package SQL;

import RDD.FirstQueryRDD;
import RDD.SecondQueryRDD;
import RDD.ThirdQueryRDD;
import org.apache.spark.api.java.JavaSparkContext;
import shared.QueryRunner;

public class SqlQueryRunner extends QueryRunner {
    public SqlQueryRunner(JavaSparkContext context) {
        super(context);
    }

    public void executeFirstQuery() {
        FirstQuerySQL query = new FirstQuerySQL();
        query.executeQuery(sparkSession);
    }
    public void executeSecondQuery() {
        SecondQuerySQL query = new SecondQuerySQL();
        query.executeQuery(sparkSession);
    }
    public void executeThirdQuery() {
        ThirdQuerySQL query = new ThirdQuerySQL();
        query.executeQuery(sparkSession);
    }

}
