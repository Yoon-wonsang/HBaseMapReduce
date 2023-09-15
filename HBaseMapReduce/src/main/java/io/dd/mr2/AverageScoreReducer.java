package io.dd.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.logging.Logger; // Logger를 추가

public class AverageScoreReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
    // Logger 인스턴스 생성
    private static final Logger logger = Logger.getLogger(AverageScoreReducer.class.getName());

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 평균을 계산하기 위한 변수 초기화
        float midtermSum = 0;
        float finalSum = 0;
        int count = 0;

        // 학생의 중간 점수와 기말 점수를 합산합니다.
        for (Text value : values) {
            String[] scores = value.toString().split("\t");
            if (scores.length == 2) {
                midtermSum += Float.parseFloat(scores[0]);
                finalSum += Float.parseFloat(scores[1]);
                ++count;
            }
        }

        // count 값이 0인 경우 로그로 출력합니다.
        if (count == 0) {
            logger.warning("Count is zero. Cannot calculate average.");
        } else {
            // 중간 점수와 기말 점수의 평균을 계산합니다.
            float midtermAverage = (float) midtermSum / count;
            float finalAverage = (float) finalSum / count;

            // HBase에 결과를 저장하기 위한 Put 객체 생성
            Put put = new Put(key.get());

            // 결과 값을 HBase의 컬럼으로 설정합니다.
            put.addColumn(Bytes.toBytes("averages"), Bytes.toBytes("midterm"), Bytes.toBytes(Float.toString(midtermAverage)));
            put.addColumn(Bytes.toBytes("averages"), Bytes.toBytes("final"), Bytes.toBytes(Float.toString(finalAverage)));

            // 결과를 HBase에 저장합니다.
            context.write(new ImmutableBytesWritable(key.get()), put);
        }
    }
}
