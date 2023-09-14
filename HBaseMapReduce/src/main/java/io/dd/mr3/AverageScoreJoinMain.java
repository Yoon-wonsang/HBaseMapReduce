package io.dd.mr3;

import java.security.PrivilegedAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

public class AverageScoreJoinMain {
    public static void main(String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.zookeeper.quorum", "adm1.dd.io,hdm2.dd.io,hdm1.dd.io");

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hbase");
        int resultCode = ugi.doAs(new PrivilegedAction<Integer>() {
            public Integer run() {
                int resultCode = -1;
                try {
                    Job job = Job.getInstance(conf, "HBase MapReduce Example");
                    TableMapReduceUtil.addDependencyJars(job);
                    job.setJarByClass(AverageScoreJoinMain.class);

                    // 맵퍼 클래스 설정
                    job.setMapperClass(AverageScoreJoinMapper.class);
                    job.setMapOutputKeyClass(CustomKey.class);
                    job.setMapOutputValueClass(Text.class);

                    // 리듀서 클래스 설정
                    job.setReducerClass(AverageScoreJoinReducer.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);

                    // CustomKey를 사용하여 정렬 및 그룹핑 설정
                    job.setSortComparatorClass(CustomKeyComparator.class);
                    job.setGroupingComparatorClass(CustomKeyGroupingComparator.class);

                    TableMapReduceUtil.initTableMapperJob(
                        "exam_scores", // 입력 HBase 테이블 이름
                        new Scan(), // 입력 데이터 스캔 설정 (전체 스캔)
                        AverageScoreJoinMapper.class, // Mapper 클래스
                        CustomKey.class, // Mapper의 출력 키 타입
                        Text.class, // Mapper의 출력 값 타입
                        job, true
                    );

                    TableMapReduceUtil.initTableReducerJob(
                        "grade_average", // 출력 HBase 테이블 이름 (새로 생성될 테이블)
                        AverageScoreJoinReducer.class, // Reducer 클래스
                        job, null, null, null, null, true
                    );

                    resultCode = job.waitForCompletion(true) ? 0 : 1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return resultCode;
            }
        });

        System.exit(resultCode);
    }
}
