import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQL {

    // 数据库连接 URL（针对 MySQL 8.0+ 补全了时区参数 serverTimezone）
    private static final String URL = "jdbc:mysql://localhost:3306/lab4?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "tdt"; // 👈 记得改这里

    public static void main(String[] args) {
        Connection conn = null;
        try {
            // 1. JDK 21 下新版驱动类名通常可省略，这里显式写出确保万无一失
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("成功连接到 MySQL 数据库！");

            // ==========================================
            // (1) 向 Student 表中添加 scofield 的记录
            // ==========================================
            String insertSql = "INSERT INTO Student (Name, English, Math, Computer) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setString(1, "scofield");
                pstmt.setInt(2, 45);
                pstmt.setInt(3, 89);
                pstmt.setInt(4, 100); // 题目(二)给出的 scofield Computer 成绩是 100
                
                int rows = pstmt.executeUpdate();
                if (rows > 0) {
                    System.out.println("成功插入 scofield 的成绩记录！");
                }
            } catch (SQLException e) {
                // 防止重复运行主程序时报主键冲突错误
                System.out.println("提示：scofield 记录可能已存在，跳过插入。信息：" + e.getMessage());
            }

            // ==========================================
            // (2) 获取 scofield 的 English 成绩信息
            // ==========================================
            String querySql = "SELECT English FROM Student WHERE Name = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(querySql)) {
                pstmt.setString(1, "scofield");
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        int englishScore = rs.getInt("English");
                        System.out.println("查询成功！scofield 的 English 成绩为: " + englishScore);
                    } else {
                        System.out.println("未找到 scofield 的记录！");
                    }
                }
            }

        } catch (ClassNotFoundException e) {
            System.err.println("找不到 MySQL 驱动，请检查 Build Path 是否导入了 JAR 包！");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("数据库操作失败！");
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}