import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.hbase.Cell;
public class HbaseUtil {
    
    // 获取 HBase 配置和连接的通用方法
    private static Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 显式指定 ZooKeeper 的地址和端口
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(configuration);
    }





    /**
     * 新需求 (1) 列出 HBase 所有的表的相关信息
     */
    public static void listTables() {
        System.out.println("====== 正在获取 HBase 所有表的信息 ======");
        try (Connection connection = getConnection();
             Admin admin = connection.getAdmin()) {
            
            // 获取所有表的描述符
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            if (tableDescriptors == null || tableDescriptors.isEmpty()) {
                System.out.println("HBase 中当前没有任何表。");
            } else {
                for (TableDescriptor td : tableDescriptors) {
                    System.out.println("-> 表名: " + td.getTableName().getNameAsString());
                    // 顺便打印出该表包含的列族，让信息更丰富
                    System.out.print("   包含的列族: ");
                    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
                        System.out.print(cfd.getNameAsString() + " ");
                    }
                    System.out.println();
                }
            }
            System.out.println("=========================================");
        } catch (IOException e) {
            System.err.println("获取表列表失败: " + e.getMessage());
        }
    }

    /**
     * 新需求 (2) 在终端打印出指定的表的所有记录数据（全表扫描）
     * @param tableName 表名
     */
    public static void scanAllData(String tableName) {
        System.out.println("====== 开始全表扫描 [" + tableName + "] 的所有记录 ======");
        try (Connection connection = getConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                boolean hasData = false;
                for (Result result : scanner) {
                    hasData = true;
                    String rowKey = Bytes.toString(result.getRow());
                    
                    // 循环打印这一行的每一个单元格（Cell）
                    for (Cell cell : result.listCells()) {
                        String family = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        long timestamp = cell.getTimestamp();
                        
                        System.out.println("RowKey: " + rowKey 
                                + " -> [" + family + ":" + qualifier + "] "
                                + "= " + value + " (版本时间戳: " + timestamp + ")");
                    }
                }
                if (!hasData) {
                    System.out.println("注意：该表当前为空表，无任何数据。");
                }
            }
            System.out.println("=================================================");
        } catch (IOException e) {
            System.err.println("全表扫描失败: " + e.getMessage());
        }
    }

    /**
     * 新需求 (3) 向已经创建好的表添加或删除指定的列族
     * @param tableName 表名
     * @param action 动作："add" 代表添加，"delete" 代表删除
     * @param columnFamily 列族名称
     */
    public static void modifyColumnFamily(String tableName, String action, String columnFamily) {
        TableName tName = TableName.valueOf(tableName);
        try (Connection connection = getConnection();
             Admin admin = connection.getAdmin()) {
            
            if (!admin.tableExists(tName)) {
                System.err.println("错误：表 " + tableName + " 不存在！");
                return;
            }

            if ("add".equalsIgnoreCase(action)) {
                System.out.println("正在向表 " + tableName + " 添加列族: " + columnFamily);
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
                // 动态添加列族
                admin.addColumnFamily(tName, cfd);
                System.out.println("====== 列族 [" + columnFamily + "] 添加成功 ======");
                
            } else if ("delete".equalsIgnoreCase(action)) {
                System.out.println("正在从表 " + tableName + " 删除列族: " + columnFamily);
                // 动态删除列族
                admin.deleteColumnFamily(tName, Bytes.toBytes(columnFamily));
                System.out.println("====== 列族 [" + columnFamily + "] 删除成功 ======");
            } else {
                System.err.println("无效的操作类型！请输入 'add' 或 'delete'");
            }
        } catch (IOException e) {
            System.err.println("修改列族结构失败: " + e.getMessage());
        }
    }

    /**
     * 新需求 (4) 清空指定的表的所有记录数据
     * HBase 没有 SQL 的 truncate 命令，标准做法是：先 disable -> 再 delete -> 重新 create
     * @param tableName 表名
     */
    public static void truncateTable(String tableName) {
        System.out.println("正在清空表 [" + tableName + "] 的所有记录数据...");
        TableName tName = TableName.valueOf(tableName);
        try (Connection connection = getConnection();
             Admin admin = connection.getAdmin()) {
            
            if (!admin.tableExists(tName)) {
                System.err.println("表不存在，无需清空。");
                return;
            }
            
            // 先获取原表的结构（保留原有的列族拓扑，防止清空后列族丢了）
            TableDescriptor oldDescriptor = admin.getDescriptor(tName);
            
            // 执行清空三板斧：下线 -> 删除 -> 重建
            System.out.println("正在下线表...");
            admin.disableTable(tName);
            System.out.println("正在删除旧表...");
            admin.deleteTable(tName);
            System.out.println("正在重新创建空表...");
            admin.createTable(oldDescriptor);
            
            System.out.println("====== 表 [" + tableName + "] 数据已成功清空！ ======");
        } catch (IOException e) {
            System.err.println("清空表失败: " + e.getMessage());
        }
    }

    /**
     * 新需求 (5) 统计表的行数（Row Count）
     * @param tableName 表名
     * @return 行数
     */
    public static long countRows(String tableName) {
        System.out.println("正在统计表 [" + tableName + "] 的总行数...");
        long rowCount = 0;
        try (Connection connection = getConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            
            Scan scan = new Scan();
            // 性能优化：统计行数时不需要看具体数据，只拿 RowKey 即可，极大节省内存和网络带宽
            scan.setFilter(new org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter());
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    rowCount++;
                }
            }
            System.out.println("====== 统计完成：表 [" + tableName + "] 共计 " + rowCount + " 行 ======");
        } catch (IOException e) {
            System.err.println("统计行数时发生异常: " + e.getMessage());
        }
        return rowCount;
    }









    /**
     * 创建表
     * @param tableName 表名
     * @param fields 存储各个列族（或者是字段）名称的数组
     */
    public static void createTable(String tableName, String[] fields) {
        System.out.println("开始准备创建表: " + tableName);
        try (Connection connection = getConnection();
             Admin admin = connection.getAdmin()) {                        
            System.out.println("成功获取 Admin 对象，正在检测表是否存在...");            TableName tName = TableName.valueOf(tableName);
            
            // 当 HBase 已经存在名为 tableName 的表的时候，先删除原有的表
            if (admin.tableExists(tName)) {
                System.out.println("表 " + tableName + " 已存在，正在执行先删除后创建...");
                admin.disableTable(tName);
                System.out.println("表已下线 (Disabled)。");
                admin.deleteTable(tName);
                System.out.println("旧表 " + tableName + " 已成功删除。");
            }            
            // 创建新表描述符（Builder 模式）
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tName);
            // 循环传入的 fields 数组，将其作为列族（Column Family）加入到表结构中
            if (fields != null && fields.length > 0) {
                for (String field : fields) {
                    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(field)).build();
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                }
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("====== 新表 " + tableName + " 创建成功！ ======");
            
        } catch (IOException e) {
            System.err.println("创建表时发生异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
         * (2) 向表中添加数据
         * @param tableName 表名
         * @param row 行键 (例如学生姓名 S_Name)
         * @param fields 列名数组，支持 "cf:qualifier" 或 "cf" 格式
         * @param values 对应的数据值数组
         */
        public static void addRecord(String tableName, String row, String[] fields, String[] values) {
            System.out.println("开始向表 " + tableName + " 的行 [" + row + "] 批量添加数据...");
            if (fields == null || values == null || fields.length != values.length) {
                System.err.println("错误：fields 数组与 values 数组长度不一致或为空！");
                return;
            }

            try (Connection connection = getConnection();
                Table table = connection.getTable(TableName.valueOf(tableName))) {
                
                Put put = new Put(Bytes.toBytes(row));
                
                for (int i = 0; i < fields.length; i++) {
                    String field = fields[i];
                    String value = values[i];
                    
                    // 解析 columnFamily 和 columnQualifier
                    byte[] cf;
                    byte[] qualifier;
                    if (field.contains(":")) {
                        String[] parts = field.split(":", 2);
                        cf = Bytes.toBytes(parts[0]);
                        qualifier = Bytes.toBytes(parts[1]);
                    } else {
                        cf = Bytes.toBytes(field);
                        qualifier = new byte[0]; // 没有限定符的情况
                    }
                    
                    put.addColumn(cf, qualifier, Bytes.toBytes(value));
                }
                
                table.put(put);
                System.out.println("====== 数据添加成功！ ======");
            } catch (IOException e) {
                System.err.println("添加数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * (3) 浏览表某一列（或某一列族）的数据
         * @param tableName 表名
         * @param column 格式为 "cf:qualifier"（查单列）或 "cf"（查整个列族）
         */
        public static void scanColumn(String tableName, String column) {
            System.out.println("开始浏览表 " + tableName + " 的列/列族 [" + column + "] 数据...");
            try (Connection connection = getConnection();
                Table table = connection.getTable(TableName.valueOf(tableName))) {
                
                Scan scan = new Scan();
                
                // 根据参数是具体列还是列族，动态调整 Scan 的范围
                if (column.contains(":")) {
                    String[] parts = column.split(":", 2);
                    scan.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
                } else {
                    scan.addFamily(Bytes.toBytes(column));
                }
                
                try (ResultScanner scanner = table.getScanner(scan)) {
                    boolean hasData = false;
                    for (Result result : scanner) {
                        hasData = true;
                        String rowKey = Bytes.toString(result.getRow());
                        
                        // 循环打印这一行中筛选出的所有单元格数据
                        for (Cell cell : result.listCells()) {
                            String family = Bytes.toString(CellUtil.cloneFamily(cell));
                            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            
                            String fullColumnName = qualifier.isEmpty() ? family : (family + ":" + qualifier);
                            System.out.println("RowKey: " + rowKey + " -> 列: " + fullColumnName + " = " + value);
                        }
                    }
                    if (!hasData) {
                        System.out.println("null (未找到该列数据或表为空)");
                    }
                }
                System.out.println("====== 浏览结束 ======");
            } catch (IOException e) {
                System.err.println("浏览数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * (4) 修改指定单元格的数据
         * @param tableName 表名
         * @param row 行键
         * @param column 列名，格式为 "cf:qualifier" 或 "cf"
         */
        public static void modifyData(String tableName, String row, String column) {
            System.out.println("开始修改表 " + tableName + " 行 [" + row + "] 列 [" + column + "] 的数据...");
            
            // 既然是修改，需要提示用户在控制台输入新的值
            Scanner scanner = new Scanner(System.in);
            System.out.print("请输入修改后的新值 (Value): ");
            String newValue = scanner.nextLine();
            
            try (Connection connection = getConnection();
                Table table = connection.getTable(TableName.valueOf(tableName))) {
                
                // HBase 中，修改和添加在 API 层面上是一样的，后写入的数据会自动覆盖/版本更新
                Put put = new Put(Bytes.toBytes(row));
                
                byte[] cf;
                byte[] qualifier;
                if (column.contains(":")) {
                    String[] parts = column.split(":", 2);
                    cf = Bytes.toBytes(parts[0]);
                    qualifier = Bytes.toBytes(parts[1]);
                } else {
                    cf = Bytes.toBytes(column);
                    qualifier = new byte[0];
                }
                
                put.addColumn(cf, qualifier, Bytes.toBytes(newValue));
                table.put(put);
                System.out.println("====== 数据修改成功！ ======");
            } catch (IOException e) {
                System.err.println("修改数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * (5) 删除指定行的记录
         * @param tableName 表名
         * @param row 行键
         */
        public static void deleteRow(String tableName, String row) {
            System.out.println("正在删除表 " + tableName + " 中行键为 [" + row + "] 的整行记录...");
            try (Connection connection = getConnection();
                Table table = connection.getTable(TableName.valueOf(tableName))) {
                
                Delete delete = new Delete(Bytes.toBytes(row));
                table.delete(delete);
                System.out.println("====== 行 [" + row + "] 已成功删除！ ======");
            } catch (IOException e) {
                System.err.println("删除数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            }
        }


    public static void main(String[] args) {        
        //强制关闭 Zookeeper 的 SASL 认证，根治未知错误引起的卡顿
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("hbase.security.authentication", "simple");
        //针对高版本 Java (JDK 17/21) 的本地安全策略降级，防止本地用户权限问题卡死
        org.apache.hadoop.security.UserGroupInformation.setConfiguration(new Configuration());


        String testTableName = "guojia_student_table";
        
        // (1): 创建表，包含列族 Score 和 info ----
        String[] testColumnFamilies = {"Score", "info"}; 
        createTable(testTableName, testColumnFamilies);
        
        // (2): 向行 "ZhangSan" 批量添加三门功课的成绩 ----
        String[] fields = {"Score:Math", "Score:Computer Science", "Score:English"};
        String[] values = {"95", "88", "90"};
        addRecord(testTableName, "ZhangSan", fields, values);
        
        // 顺便给李四也加点数据，用来做对比测试
        addRecord(testTableName, "LiSi", new String[]{"Score:Math", "info:age"}, new String[]{"72", "20"});

        // (3): 浏览某一具体列的数据 (例如 Score:Math) ----
        scanColumn(testTableName, "Score:Math");
        
        // (3) 拓展: 浏览整个列族的数据 (例如 Score) ----
        scanColumn(testTableName, "Score");

        // (4): 修改张三 Computer Science 的成绩 (会在控制台等待你输入) ----
        modifyData(testTableName, "ZhangSan", "Score:Computer Science");
        
        // 修改完验证一下看变了没有
        scanColumn(testTableName, "Score");

        // (5): 删除李四这行记录 ----
        deleteRow(testTableName, "LiSi");
        
        // 全表浏览验证李四是否消失（通过传入一个肯定存在的列族来观察全表）
        System.out.println("\n--- 最终检查整表 Score 列族状况 ---");
        scanColumn(testTableName, "Score");



        // 1. 测试列出所有表
        listTables();
        // 2. 测试全表数据扫描打印
        scanAllData(testTableName);
        // 3. 测试统计当前行数
        countRows(testTableName);
        // 4. 测试动态加一个列族 "ext_info"
        modifyColumnFamily(testTableName, "add", "ext_info");
        listTables(); // 查看表结构里是不是多了 ext_info
        // 5. 测试动态删掉这个列族 "ext_info"
        modifyColumnFamily(testTableName, "delete", "ext_info");
        // 6. 测试清空整张表的数据
        truncateTable(testTableName);
        // 7. 清空后再次检查行数（应该为 0）
        countRows(testTableName);




        String tableName = "Student";
        String[] columnFamilies = {"info", "score"}; 
        createTable(tableName, columnFamilies);

        // ==================== 学生 1: 2015001 (Zhangsan) ====================
        // info 列族数据
        String[] fields1_info = {"info:S_Name", "info:S_Sex", "info:S_Age"};
        String[] values1_info = {"Zhangsan", "male", "23"};
        addRecord(tableName, "2015001", fields1_info, values1_info);
        
        // score 列族数据（选了 Math 和 English）
        String[] fields1_score = {"score:Math", "score:English"};
        String[] values1_score = {"86", "69"};
        addRecord(tableName, "2015001", fields1_score, values1_score);


        // ==================== 学生 2: 2015002 (Mary) ====================
        // info 列族数据
        String[] fields2_info = {"info:S_Name", "info:S_Sex", "info:S_Age"};
        String[] values2_info = {"Mary", "female", "22"};
        addRecord(tableName, "2015002", fields2_info, values2_info);
        
        // score 列族数据（选了 Computer Science 和 English）
        String[] fields2_score = {"score:Computer Science", "score:English"};
        String[] values2_score = {"77", "99"};
        addRecord(tableName, "2015002", fields2_score, values2_score);


        // ==================== 学生 3: 2015003 (Lisi) ====================
        // info 列族数据
        String[] fields3_info = {"info:S_Name", "info:S_Sex", "info:S_Age"};
        String[] values3_info = {"Lisi", "male", "24"};
        addRecord(tableName, "2015003", fields3_info, values3_info);
        
        // score 列族数据（选了 Math 和 Computer Science）
        String[] fields3_score = {"score:Math", "score:Computer Science"};
        String[] values3_score = {"98", "95"};
        addRecord(tableName, "2015003", fields3_score, values3_score);


        // 3. 最终结果验证：调用你之前的全表扫描函数，将最终插入的数据在控制台完美打印
        System.out.println("\n====== [结果验证] 打印 HBase 表中最终的数据视图 ======");
        scanAllData(tableName);





    
    }
}