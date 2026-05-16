import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;//Url小写的气死我了气死我了气死我了
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.net.URL;

/**
 * 使用标准 Java URL 协议访问 HDFS 的工具类
 */
public class HdfsUrlReader {
    // 静态代码块：确保 JVM 能够识别 hdfs:// 协议
    static {
        try {
            // 注册 Hadoop 的 URL 流处理器工厂
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Error e) {
            // 防止在某些环境中重复注册抛出错误
            System.out.println("URLFactory 已注册过，跳过。");
        }
    }

    /**
     * 通过 URL 方式读取并输出文件内容
     * @param hdfsUrl 完整的 HDFS 路径，例如 "hdfs://localhost:9000/file1.txt"
     */
    public static void readByUrl(String hdfsUrl) {
        try (InputStream in = new URL(hdfsUrl).openStream()) {
            System.out.println("--- " + hdfsUrl + " ---");
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            System.err.println("URL 读取失败: " + e.getMessage());
        }
    }
}