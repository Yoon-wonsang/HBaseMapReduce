//package io.dd.ws;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellScanner;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.junit.Before;
//import org.junit.Test;
//
//public class HBaseJavaTest {
//
//    private Configuration conf;
//
//    @Before
//    public void setup() {
//        conf = HBaseConfiguration.create();
//        conf.set("zookeeper.znode.parent", "/hbase");
//        conf.set("hbase.zookeeper.quorum", "adm1.dd.io,hdm2.dd.io,hdm1.dd.io");
//        
//    }
//
//    @Test
//    public void connectTest() {
//        try {
//            System.out.println(conf);
//            Connection connection = ConnectionFactory.createConnection(conf);
//            System.out.println(connection);
//            Table table = connection.getTable(TableName.valueOf("scores"));
//            System.out.println(table);
//            Result result = table.get(new Get(Bytes.toBytes("student1")));
//            System.out.println(result);
//            CellScanner cellScanner = result.cellScanner();
//            System.out.println(cellScanner);
//            while (result.advance()) {
//                Cell cell = cellScanner.current();
//                String cf = new String(CellUtil.cloneFamily(cell));
//                String q = new String(CellUtil.cloneQualifier(cell));
//                String v = new String(CellUtil.cloneValue(cell));
//                String r = new String(CellUtil.cloneRow(cell));
//                System.out.println("cf = " + cf);
//                System.out.println("q  = " + q);
//                System.out.println("v  = " + v);
//                System.out.println("r  = " + r);
//            }
//            connection.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}
