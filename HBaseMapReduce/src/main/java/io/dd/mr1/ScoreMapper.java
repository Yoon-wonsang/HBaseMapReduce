package io.dd.mr1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class ScoreMapper extends TableMapper<Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(ImmutableBytesWritable rowKey, Result value, Context context)
            throws IOException, InterruptedException {
        // 행 키와 열 값 가져오기
        byte[] subjectBytes = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("subject"));
        byte[] scoreBytes = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("score"));
        
        // byte 배열을 문자열로 변환
        String subject = Bytes.toString(subjectBytes);
        String score = Bytes.toString(scoreBytes);

        // 과목이 'Math'이고 점수가 80 이상인 경우만 출력
        if ("Math".equals(subject) && Integer.parseInt(score) >= 80) {
            outKey.set(subject);    // 출력 키 설정
            outValue.set(score);    // 출력 값 설정
            context.write(outKey, outValue);   // 출력 데이터 기록
        }
    }
}