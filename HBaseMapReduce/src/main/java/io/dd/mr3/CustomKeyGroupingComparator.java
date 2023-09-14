package io.dd.mr3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyGroupingComparator extends WritableComparator {

    protected CustomKeyGroupingComparator() {
        // WritableComparator의 생성자 호출
        super(CustomKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // 학년 정보를 내림차순으로 정렬합니다.
        CustomKey key1 = (CustomKey) w1;
        CustomKey key2 = (CustomKey) w2;

        int grade1 = mapGradeToNumber(key1.getYear());
        int grade2 = mapGradeToNumber(key2.getYear());

        // 내림차순 정렬
        int result = Integer.compare(grade2, grade1);

        return result;
    }

    private int mapGradeToNumber(String grade) {
        // 학년 문자열을 숫자로 매핑합니다.
        if ("1st".equals(grade)) {
            return 1;
        } else if ("2nd".equals(grade)) {
            return 2;
        } else if ("3rd".equals(grade)) {
            return 3;
        } else if ("4th".equals(grade)) {
            return 4;
        } else {
            // 기타 학년 정보 처리
            return 0;
        }
    }
}
