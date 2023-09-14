package io.dd.mr3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyComparator extends WritableComparator {

    protected CustomKeyComparator() {
        // WritableComparator의 생성자 호출
        super(CustomKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // CustomKey의 compareTo 메서드를 호출하여 비교합니다.
        CustomKey key1 = (CustomKey) w1;
        CustomKey key2 = (CustomKey) w2;
        
        return key1.compareTo(key2);
    }
}
