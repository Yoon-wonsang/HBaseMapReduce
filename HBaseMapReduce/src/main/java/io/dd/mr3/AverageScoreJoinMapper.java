package io.dd.mr3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AverageScoreJoinMapper extends TableMapper<CustomKey, Text> {
    private Table studentInfoTable;
    private Text outValue = new Text();
    private CustomKey customKey = new CustomKey();
    private Map<String, String> studentInfoData = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.zookeeper.quorum", "adm1.dd.io,hdm2.dd.io,hdm1.dd.io");
        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("student_info");
        studentInfoTable = connection.getTable(tableName);

        ResultScanner scanner = studentInfoTable.getScanner(Bytes.toBytes("info"));
        for (Result result : scanner) {
            String studentId = Bytes.toString(result.getRow());
            String year = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("year")));
            studentInfoData.put(studentId, year);
        }
        scanner.close();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String studentId = Bytes.toString(key.get());

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

        String grade = studentInfoData.get(studentId);

        float midtermAverage = (chemistryMid + englishMid + historyMid + mathMid + scienceMid) / 5.0f;
        float finalAverage = (chemistryFinal + englishFinal + historyFinal + mathFinal + scienceFinal) / 5.0f;

        customKey.setYear(grade);
        customKey.setStudentId(studentId);

        outValue.set(new Text(grade + "\t" + midtermAverage + "\t" + finalAverage));
        context.write(customKey, outValue);
    }

    private int getScore(Result value, String family, String qualifier) {
        byte[] scoreBytes = value.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (scoreBytes != null) {
            String scoreValue = Bytes.toString(scoreBytes);
            try {
                return Integer.parseInt(scoreValue);
            } catch (NumberFormatException e) {
                return 0;
            }
        } else {
            return 0;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        studentInfoTable.close();
    }
}
