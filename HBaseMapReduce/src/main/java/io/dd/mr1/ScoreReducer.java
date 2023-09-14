package io.dd.mr1;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class ScoreReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        
        // 각 과목의 점수를 합산하고 개수를 세기
        for (Text value : values) {
            int score = Integer.parseInt(value.toString());
            sum += score;
            count++;
        }
        
        // 평균 점수 계산
        double average = (double) sum / count;
        
        // 출력 테이블에 결과 저장
        Put put = new Put(key.toString().getBytes());
        put.addColumn("info".getBytes(), "average".getBytes(), String.valueOf(average).getBytes());
        context.write(null, put);
    }
}
