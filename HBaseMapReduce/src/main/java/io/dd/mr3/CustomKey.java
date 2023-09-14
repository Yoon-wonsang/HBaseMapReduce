package io.dd.mr3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {
    private String year;        // 학년 정보
    private String studentId;   // 학생 ID

    public CustomKey() {
        // 기본 생성자
    }

    public CustomKey(String year, String studentId) {
        this.year = year;
        this.studentId = studentId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 데이터를 직렬화하여 출력 스트림에 쓰는 메서드
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(studentId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 데이터를 역직렬화하여 입력 스트림에서 읽는 메서드
        year = dataInput.readUTF();
        studentId = dataInput.readUTF();
    }

    @Override
    public int compareTo(CustomKey other) {
        // 학년을 내림차순으로 비교하고, 같은 학년이면 학생 ID를 오름차순으로 비교합니다.
        int grade1 = mapGradeToNumber(this.year);
        int grade2 = mapGradeToNumber(other.year);

        int result = Integer.compare(grade2, grade1);

        if (result == 0) {
            result = this.studentId.compareTo(other.studentId);
        }

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

    public void setYear(String year) {
        this.year = year;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public String getYear() {
        return year;
    }

    public String getStudentId() {
        return studentId;
    }
}
