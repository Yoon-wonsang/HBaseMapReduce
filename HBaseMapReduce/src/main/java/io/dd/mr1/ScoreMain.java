package io.dd.mr1;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

public class ScoreMain {
	public static void main(String[] args) throws Exception {
		final Configuration conf = HBaseConfiguration.create();
		conf.set("zookeeper.znode.parent", "/hbase");
		conf.set("hbase.zookeeper.quorum", "adm1.dd.io,hdm2.dd.io,hdm1.dd.io");

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hbase");
		int resultCode = ugi.doAs(new PrivilegedAction<Integer>() {
			@Override
			public Integer run() {
				int resultCode = -1;
				try {
					Job job = Job.getInstance(conf, "HBase MapReduce Example");
					TableMapReduceUtil.addDependencyJars(job);
					job.setJarByClass(ScoreMain.class);

					// Mapper 설정
					TableMapReduceUtil.initTableMapperJob("scores", // 입력 HBase 테이블 이름
							new Scan(), // 입력 데이터 스캔 설정 (전체 스캔)
							ScoreMapper.class, // Mapper 클래스
							Text.class, // Mapper의 출력 키 타입
							Text.class, // Mapper의 출력 값 타입
							job, false);

					// Reducer 설정
					TableMapReduceUtil.initTableReducerJob("output_table", // 출력 HBase 테이블 이름
							ScoreReducer.class, // Reducer 클래스
							job, null, null, null, null, false);
					resultCode = job.waitForCompletion(true) ? 0 : 1;
				} catch (IOException | ClassNotFoundException | InterruptedException e) {
					e.printStackTrace();
				}

				return resultCode;
			}
		});

		// 작업 실행
		System.exit(resultCode);
	}
}
