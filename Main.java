

public class Main {
    public static void main(String[] args) throws Exception
    { 
        HdfsUtil dfs = new HdfsUtil();

        System.out.println("（1） 向 HDFS 中上传任意文本文件，如果指定的文件在 HDFS 中已经存在，则由用户来指定是追加到原有文件末尾还是覆盖原有的文件；");
        dfs.uploadFile("./test/file1.txt", "/file1.txt", false);
        dfs.uploadFile("./test/file2.txt", "/file2.txt", false);
        dfs.cat("/file1.txt");
        dfs.uploadFile("./test/file2.txt", "/file1.txt", true);
        dfs.cat("/file1.txt");
        dfs.uploadFile("./test/file1.txt", "/file1.txt", false);
        dfs.cat("/file1.txt");
        
        System.out.println("（2） 从 HDFS 中下载指定文件，如果本地文件与要下载的文件名称相同，则自动对下载的文件重命名；");
        dfs.downloadFile("/file1.txt", "./test");
        
        System.out.println("（3） 将 HDFS 中指定文件的内容输出到终端中；");
        dfs.cat("/file1.txt");
        
        System.out.println("（4） 显示 HDFS 中指定的文件的读写权限、大小、创建时间、路径等信息；");
        dfs.ls("/file1.txt",false);
        
        System.out.println("（5） 给定 HDFS 中某一个目录，输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息，如果该文件是目录，则递归输出该目录下所有文件相关信息；");
        dfs.ls("/",true);
        
        System.out.println("（6） 提供一个 HDFS 内的文件的路径，对该文件进行创建和删除操作。如果文件所在目录不存在，则自动创建目录；");
        dfs.touch("/test.txt");
        dfs.rm("/test.txt");
        dfs.rm("/test.txt");
        dfs.touch("/a/b/c/test.txt");
        dfs.showTree("/",true);
        
        System.out.println("（7） 提供一个 HDFS 的目录的路径，对该目录进行创建和删除操作。创建目录时，如果目录文件所在目录不存在，则自动创建相应目录；删除目录时，由用户指定当该目录不为空时是否还删除该目录；");
        dfs.mkdir("/a1/a2/a3");
        dfs.touch("/a1/a2/a3/test.txt");
        dfs.rmdir("/a1");
        dfs.showTree("/",true);
        
        System.out.println("（8） 向 HDFS 中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾；");
        dfs.appendContent("/file1.txt", "添加到结尾", false);
        dfs.cat("/file1.txt");
        dfs.appendContent("/file1.txt", "添加到开头", true);
        dfs.cat("/file1.txt");
        
        System.out.println("（9） 删除 HDFS 中指定的文件；");
        dfs.rm("/file1.txt");
        
        System.out.println("（10） 在 HDFS 中，将文件从源路径移动到目的路径。");
        dfs.mv("/file2.txt", "/a/b/c");
        dfs.showTree("/",true);


        dfs.uploadFile("./test/file1.txt", "/file1.txt", false);
        // 任务（二）：自定义流读取
        System.out.println("（二）验证自定义 MyFSDataInputStream 按行读取：");
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path("/file1.txt");
        try (MyFSDataInputStream myIn = new MyFSDataInputStream(dfs.getFs().open(p))) {
            String line;
            while ((line = myIn.readLine()) != null) {
                System.out.println("读取到一行: " + line);
            }
        }

        // 任务（三）：URL 方式读取
        System.out.println("（三）验证 java.net.URL 方式输出文件：");
        HdfsUrlReader.readByUrl("hdfs://localhost:9000/file1.txt");

        // System.out.println("关之前再看一眼");
        // dfs.showTree("/",true);  
        dfs.close();
    }
}
