

public class Main {
    public static void main(String[] args) throws Exception
    { 
        HdfsUtil dfs = new HdfsUtil();
        // dfs.uploadFile("./test/file1.txt","/file1.txt"  , false);
        // dfs.downloadFile("/file1.txt", "./test");
        // dfs.cat("/file1.txt");
        // dfs.mkdir("/test/a/a/a/a/a/a/a/a/a");
        // dfs.rmdir("/test");





        System.out.println("关之前再看一眼");
        dfs.showTree("/",true);  
        dfs.close();
    }
}
