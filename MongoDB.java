import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;

public class MongoDB {

    public static void main(String[] args) {
        // 1. 连接本地 MongoDB 服务（默认端口 27017）
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            
            // 2. 获取数据库和集合
            MongoDatabase database = mongoClient.getDatabase("lab4");
            MongoCollection<Document> collection = database.getCollection("student");
            System.out.println("成功连接到 MongoDB student 集合！");

            // ==========================================
            // (1) 添加数据：scofield 的嵌套文档
            // ==========================================
            // 构造内部的 score 文档
            Document scoreDoc = new Document("English", 45)
                                    .append("Math", 89)
                                    .append("Computer", 100);
            
            // 构造主文档
            Document studentDoc = new Document("name", "scofield")
                                      .append("score", scoreDoc);

            // 检查是否已存在，不存在则插入（防止重复运行报错）
            if (collection.find(Filters.eq("name", "scofield")).first() == null) {
                collection.insertOne(studentDoc);
                System.out.println(" 成功插入 scofield 的文档数据！");
            } else {
                System.out.println("提示：scofield 文档已存在，跳过插入。");
            }

            // ==========================================
            // (2) 获取 scofield 的所有成绩信息 (只显示 score 列)
            // ==========================================
            // find() 匹配 name，projection() 包含 score 并排除 _id
            Document result = collection.find(Filters.eq("name", "scofield"))
                                        .projection(Projections.fields(Projections.include("score"), Projections.excludeId()))
                                        .first();

            if (result != null) {
                System.out.println(" 查询成功！只显示 score 列结果: " + result.toJson());
            } else {
                System.out.println("未找到 scofield 的记录！");
            }

        } catch (Exception e) {
            System.err.println("MongoDB 连接或操作失败！");
            e.printStackTrace();
        }
    }
}