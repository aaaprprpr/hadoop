import redis.clients.jedis.Jedis;
import java.util.Map;

public class Redis {

    public static void main(String[] args) {
        // 1. 连接本地的 Redis 服务（默认端口 6379）
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            System.out.println("成功连接到 Redis 服务器！");

            // ==========================================
            // (1) 添加数据：scofield (English:45, Math:89, Computer:100)
            // ==========================================
            String key = "student.scofield";
            
            // 使用 Map 构建哈希内部的键值对
            Map<String, String> scoreMap = Map.of(
                "English", "45",
                "Math", "89",
                "Computer", "100"
            );
            
            // 写入 Redis 哈希
            jedis.hset(key, scoreMap);
            System.out.println("👉 成功插入 scofield 的哈希数据！");

            // ==========================================
            // (2) 获取 scofield 的 English 成绩信息
            // ==========================================
            String englishScore = jedis.hget(key, "English");
            
            if (englishScore != null) {
                System.out.println("👉 查询成功！scofield 的 English 成绩为: " + englishScore);
            } else {
                System.out.println("未找到 scofield 的 English 成绩记录！");
            }

        } catch (Exception e) {
            System.err.println("Redis 连接或操作失败！请确保 redis-server 已经在后台启动！");
            e.printStackTrace();
        }
    }
}