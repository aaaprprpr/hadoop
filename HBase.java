import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class HBase {

    public static void main(String[] args) {
        // 1. 创建 HBase 配置对象
        Configuration conf = HBaseConfiguration.create();
        // 如果你的 HBase 用的不是默认的 localhost，请把下面改成你的配置
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 2. 建立连接并执行操作
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            
            TableName tableName = TableName.valueOf("Student");
            
            // 校验表是否存在
            if (!admin.tableExists(tableName)) {
                System.err.println("错误：HBase 中不存在 Student 表，请先在 Shell 中创建它！");
                return;
            }

            try (Table table = connection.getTable(tableName)) {
                System.out.println("成功连接到 HBase Student 表！");

                // ==========================================
                // (1) 添加数据：scofield (English:45, Math:89, Computer:100)
                // ==========================================
                // RowKey 设为 scofield
                Put put = new Put(Bytes.toBytes("scofield"));
                // 列族为 score，添加三列
                put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("English"), Bytes.toBytes("45"));
                put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("Math"), Bytes.toBytes("89"));
                put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("Computer"), Bytes.toBytes("100"));
                
                table.put(put);
                System.out.println("👉 成功插入 scofield 的成绩数据！");

                // ==========================================
                // (2) 获取 scofield 的 English 成绩信息
                // ==========================================
                Get get = new Get(Bytes.toBytes("scofield"));
                // 限定只获取 score:English 这一列
                get.addColumn(Bytes.toBytes("score"), Bytes.toBytes("English"));
                
                Result result = table.get(get);
                // 从返回结果中提取值
                byte[] valBytes = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("English"));
                
                if (valBytes != null) {
                    String englishScore = Bytes.toString(valBytes);
                    System.out.println("👉 查询成功！scofield 的 English 成绩为: " + englishScore);
                } else {
                    System.out.println("未找到 scofield 的 English 成绩记录！");
                }
            }

        } catch (IOException e) {
            System.err.println("HBase 连接或操作失败！请确保 Hadoop 和 HBase 服务已完全启动！");
            e.printStackTrace();
        }
    }
}