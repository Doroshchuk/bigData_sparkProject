import DataFrame.DataFrameQueryRunner;
import RDD.RddQueryRunner;
import SQL.SqlQueryRunner;
import benchmark.QueryRunnerBenchmark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        DataFrameQueryRunner dataFrameQueryRunner = new DataFrameQueryRunner(sparkContext);
        RddQueryRunner rddQueryRunner = new RddQueryRunner(sparkContext);
        SqlQueryRunner sqlQueryRunner = new SqlQueryRunner(sparkContext);

        QueryRunnerBenchmark queryRunnerBenchmark = new QueryRunnerBenchmark();
        outputBenchmarkResult("Data Frame", queryRunnerBenchmark.benchmark(dataFrameQueryRunner));
        outputBenchmarkResult("RDD", queryRunnerBenchmark.benchmark(rddQueryRunner));
        outputBenchmarkResult("SQL", queryRunnerBenchmark.benchmark(sqlQueryRunner));
    }

    private static void outputBenchmarkResult(String queryStrategyName, long[] msResults) {
        for (int i = 0; i < msResults.length; i++) {
            System.out.println(queryStrategyName + " query number " + (i + 1) + " execution time is " + msResults[i] + " millis");
        }
    }
}