package benchmark;

import shared.QueryRunner;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class QueryRunnerBenchmark {
    public long[] benchmark(QueryRunner queryRunner) {
        return new long []{benchmarkFirst(queryRunner), benchmarkSecond(queryRunner), benchmarkThird(queryRunner)};
    }

    private long benchmarkFirst(QueryRunner queryRunner) {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end;
        queryRunner.executeFirstQuery();

        end = LocalDateTime.now();
        return ChronoUnit.MILLIS.between(start, end);
    }

    private long benchmarkSecond(QueryRunner queryRunner) {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end;
        queryRunner.executeSecondQuery();

        end = LocalDateTime.now();
        return ChronoUnit.MILLIS.between(start, end);
    }

    private long benchmarkThird(QueryRunner queryRunner) {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end;
        queryRunner.executeThirdQuery();

        end = LocalDateTime.now();
        return ChronoUnit.MILLIS.between(start, end);
    }

}
