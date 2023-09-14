package io.dd.mr4;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AverageScoreJoinReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float midtermFinalSum = 0;
        int count = 0;
        String grade = null;
        float midtermAverage = 0; // 중간고사 평균 초기화
        float finalAverage = 0; // 기말고사 평균 초기화

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            midtermAverage = Float.parseFloat(parts[1]);
            finalAverage = Float.parseFloat(parts[2]);
            grade = parts[0];

            midtermFinalSum += midtermAverage + finalAverage;
            count++;
        }

        if (grade != null) {
            // 학년 정보가 변경되지 않는다고 가정하므로, 학년 정보가 null이 아닌 경우에만 결과를 출력합니다.
            writeResult(context, key.get(), midtermFinalSum, count, grade, midtermAverage, finalAverage);
        }
    }

    private void writeResult(Context context, byte[] studentId, float midtermFinalSum, int count, String grade,
                             float midtermAverage, float finalAverage)
            throws IOException, InterruptedException {
        Put put = new Put(studentId);
        put.addColumn(Bytes.toBytes("average"), Bytes.toBytes("midterm"), Bytes.toBytes(Float.toString(midtermAverage)));
        put.addColumn(Bytes.toBytes("average"), Bytes.toBytes("final"), Bytes.toBytes(Float.toString(finalAverage)));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("total"), Bytes.toBytes(Float.toString(midtermFinalSum)));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("grade"), Bytes.toBytes(grade));

        context.write(new ImmutableBytesWritable(studentId), put);
    }
}
