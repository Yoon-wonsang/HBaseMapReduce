package io.dd.mr2;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AverageScoreMapper extends TableMapper<ImmutableBytesWritable, Text> {
	private Text outValue = new Text();
	
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        // 키로 학생 ID를 가져옵니다.
    	String studentId = Bytes.toString(key.get());

        // 각 과목의 중간고사와 기말고사 점수를 가져옵니다.
        int chemistryMid = getScore(value, "chemistry", "mid");
        int chemistryFinal = getScore(value, "chemistry", "final");

        int englishMid = getScore(value, "english", "mid");
        int englishFinal = getScore(value, "english", "final");

        int historyMid = getScore(value, "history", "mid");
        int historyFinal = getScore(value, "history", "final");

        int mathMid = getScore(value, "math", "mid");
        int mathFinal = getScore(value, "math", "final");

        int scienceMid = getScore(value, "science", "mid");
        int scienceFinal = getScore(value, "science", "final");

        // 중간 점수와 기말 점수의 평균을 계산합니다.
        float midtermAverage = (chemistryMid + englishMid + historyMid + mathMid + scienceMid) / 5.0f;
        float finalAverage = (chemistryFinal + englishFinal + historyFinal + mathFinal + scienceFinal) / 5.0f;
        
        String outputValue = midtermAverage + "\t" + finalAverage;

        // 결과를 출력합니다. 학생 ID를 Row Key로 사용하고, 평균 점수를 Text로 출력합니다.
        outValue.set(outputValue);
        context.write(key, outValue);
    }

    // 열 값을 가져오고 int로 변환하는 도우미 메서드
    private int getScore(Result value, String family, String qualifier) {
        byte[] scoreBytes = value.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (scoreBytes != null) {
            String scoreValue = new String(scoreBytes);
            try {
                return Integer.parseInt(scoreValue); // 문자열을 정수로 파싱
            } catch (NumberFormatException e) {
                // 파싱 오류 시 예외 처리
                return 0; // 기본값으로 0을 반환
            }
        } else {
            return 0; // 값이 없는 경우 기본값으로 0을 반환
        }
    }
}
