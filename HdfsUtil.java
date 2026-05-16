import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;


public class HdfsUtil {
    private FileSystem fs;
    private Scanner sc = new Scanner(System.in);
    private boolean idHadoopRunning(String host,int port)throws Exception{
        try(java.net.Socket socket = new java.net.Socket()){
            socket.connect(new java.net.InetSocketAddress(host, port),500);
            return true;
        }catch(Exception e){
            return false;
        }
    }
    private void startHadoop() throws Exception{ 
        System.out.println("检测集群状态并尝试启动");
        ProcessBuilder pb =new ProcessBuilder("bash","-c","start-all.sh");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try(BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))){
            String line;
            while((line=br.readLine())!=null){
                System.out.println("[Hadoop Shell"+line);
            }
        }
        p.waitFor();
    }
    public HdfsUtil() throws Exception {
        try{
            if(idHadoopRunning("localhost",9000)){
                System.out.println("集群已启动");
            }else{
                System.out.println("集群未启动，尝试启动");
                startHadoop();
                Thread.sleep(20000);
            }            

            Configuration conf = new Configuration();
            conf.set("dfs.support.append", "true");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            URI uri = new URI("hdfs://localhost:9000");
            fs = FileSystem.get(uri, conf, "guojia");
            // 强制退出安全模式
            if (fs instanceof org.apache.hadoop.hdfs.DistributedFileSystem) {
                org.apache.hadoop.hdfs.DistributedFileSystem adminFs = (org.apache.hadoop.hdfs.DistributedFileSystem) fs;
                // 设置安全模式动作为 LEAVE（离开）
                adminFs.setSafeMode(org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
                System.out.println("检测并强制退出 HDFS 安全模式成功。");
            }
            System.out.println("连接并初始化成功");
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("初始化爆炸");
        }
    }
    public void close() throws Exception {
        try{
            if(fs != null){
                fs.close();
                System.out.println("关了");
            }
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("关闭时候炸了");
        }        
    }

//（1） 向 HDFS 中上传任意文本文件，如果指定的文件在 HDFS 中已经存在，则由用户来指定是追加到原有文件末尾还是覆盖原有的文件；
    /**
     * @param localPath 本地文件路径
     * @param hdfsPath hdfs文件路径
     * @param isAppend 是否追加,true为追加,false为覆盖
     */
    public void uploadFile(String localPath, String hdfsPath,boolean isAppend) throws Exception { 
        Path src=new Path(localPath);
        Path dst=new Path(hdfsPath);
        if(fs.exists(dst)){
            if(isAppend){
                System.out.println("文件存在，正在追加");
                try(InputStream in = new BufferedInputStream(new FileInputStream(localPath));
                OutputStream out = fs.append(dst)){
                    IOUtils.copyBytes(in, out, 4096, true);
                }
            }else{ 
                System.out.println("文件已存在，正在覆盖");
                fs.copyFromLocalFile(false,true,src, dst);
            }
        }else{
            System.out.println("文件不存在，正在上传");
            fs.copyFromLocalFile(src, dst);
        }
        System.out.println("传完了");
    }

//（2） 从 HDFS 中下载指定文件，如果本地文件与要下载的文件名称相同，则自动对下载的文件重命名；
    /**
     * @param hdfsPath hdfs文件路径
     * @param localPath 本地目标文件路径
     */
    public void downloadFile(String hdfsPath, String localPath)throws Exception{
        Path src=new Path(hdfsPath);
        String fileName=src.getName();
        int dotIndex = fileName.lastIndexOf(".");
        String nameOnly = (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
        String extension = (dotIndex == -1) ? "" : fileName.substring(dotIndex);
        File localFile=new File(localPath,fileName);
        int count=1;
        while(localFile.exists()){
            localFile=new File(localPath,nameOnly+"("+count+")"+extension);
            count++;
        }
        try{
            fs.copyToLocalFile(false,src, new Path(localFile.getAbsolutePath()),true);
            System.out.println("下载到："+localFile.getAbsolutePath());            
        }catch(Exception e){
            e.printStackTrace();
            System.err.println("下载失败");
        }
    }

//（3） 将 HDFS 中指定文件的内容输出到终端中；
    /**
     * 显示文件内容
     * @param hdfsPath hdfs文件路径
     */
    public void cat(String hdfsPath)throws Exception{ 
        Path path=new Path(hdfsPath);
        if(!fs.exists(path) || fs.getFileStatus(path).isDirectory()){
            System.err.println("文件不存在");
            return;
        }
        try(FSDataInputStream in = fs.open(path)){
            System.out.println("---文件内容开始---");
            IOUtils.copyBytes(in, System.out,4096,false);
            System.out.println("\n---文件内容结束---");
        }catch(IOException e){
            System.err.println("读取文件失败"+e.getMessage());
            throw e;
        }  
    }

//（4） 显示 HDFS 中指定的文件的读写权限、大小、创建时间、路径等信息；
//（5） 给定 HDFS 中某一个目录，输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息，如果该文件是目录，则递归输出该目录下所有文件相关信息；
    /**
     * 列出文件信息
     * @param hdfsPath hdfs文件路径
     * @param recursive 是否递归
     */
    public void ls(String hdfsPath,boolean recursive)throws Exception{ 
        Path path=new Path(hdfsPath);
        if(!assertExists(path)) return;

        FileStatus status=fs.getFileStatus(path);
        if(status.isDirectory()){
            listDirectory(path, recursive);
        }else{
            printFileStatue(status);
        }
    }
    // 打印文件信息
    private void printFileStatue(FileStatus status){
        java.text.SimpleDateFormat sdf=new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm");
        System.out.printf("%s | 副本:%d | 大小:%d bytes | 修改时间:%s | 路径:%s%n",
            status.getPermission().toString(),
            status.getReplication(),
            status.getLen(),
            sdf.format(new java.util.Date(status.getModificationTime())),
            status.getPath().toUri().getPath()
        );
    }
    // 列出目录
    private void listDirectory(Path path,boolean recursive) throws Exception{ 
        FileStatus[] statuses=fs.listStatus(path);
        if(statuses==null)return;
        for(FileStatus s : statuses){
            printFileStatue(s);
            if(recursive && s.isDirectory()){
                listDirectory(s.getPath(), recursive);
            }
        }         
    }

//（6） 提供一个 HDFS 内的文件的路径，对该文件进行创建和删除操作。如果文件所在目录不存在，则自动创建目录；
//（9） 删除 HDFS 中指定的文件；
    /**
     * 创建文件
     * @param path 文件路径
     */
    public void touch(String hdfsPath)throws IOException{ 
        Path path=new Path(hdfsPath);
        if(!assertNotExists(path)) return;  

        fs.mkdirs(path.getParent());
        fs.createNewFile(path);
        System.out.println("创建成功");
    }

    /**
     * 删除文件/空目录
     * @param path 路径
     */
    public void rm(String hdfsPath)throws IOException{ 
        Path path=new Path(hdfsPath);
        if(!assertExists(path)) return;
        
        if(fs.getFileStatus(path).isDirectory()){
            if(fs.listStatus(path).length>0){
                System.err.println("目录非空，请使用rmdir删除");
                return;
            }else{
                fs.delete(path,false);
                System.out.println("删除成功");
            }
        }else{
            fs.delete(path,false);
            System.out.println("删除成功");
        }
        
    }

//（7） 提供一个 HDFS 的目录的路径，对该目录进行创建和删除操作。创建目录时，如果目录文件所在目录不存在，则自动创建相应目录；删除目录时，由用户指定当该目录不为空时是否还删除该目录；
    /**
     * 创建目录
     * @param path 目录路径
     */
    public void mkdir(String hdfsPath)throws Exception{ 
        Path path=new Path(hdfsPath);
        if(!assertNotExists(path)) return;  
        fs.mkdirs(path);
        System.out.println("创建成功");
    }
    /**
     * 删除目录
     * @param path 目录路径
     */
    public void rmdir(String hdfsPath)throws IOException{ 
        Path path=new Path(hdfsPath);
        if(!assertExists(path)) return;

        if(fs.listStatus(path).length>0){
            System.out.println("目录非空，确认删除？(y/n))");
            if(!sc.nextLine().equalsIgnoreCase("y")){
                System.out.println("取消删除");
                return;
            }
        }
        fs.delete(path,true);
        System.out.println("删除成功");
    }

//（8） 向 HDFS 中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾；
    /**
     * @param hdfsPath hdfs文件路径
     * @param content 要追加的内容
     * @param asStart 是否在文件开头追加,true为开头,false为结尾
     */
    public void appendContent(String hdfsPath,String content,boolean asStart)throws Exception{
        Path path=new Path(hdfsPath);
        if(!assertExists(path)) return;        

        if(asStart){
            Path tempPath=new Path(hdfsPath+"_tmp");
            try(OutputStream out = fs.create(tempPath)){
                out.write(content.getBytes());
                try(InputStream in = fs.open(path)){
                    IOUtils.copyBytes(in, out, 4096, false);
                }
            }
            fs.delete(path,true);
            fs.rename(tempPath, path);
            System.out.println("开头追加成功");
        }else{
            try(OutputStream out = fs.append(path)){
                out.write(content.getBytes());
                System.out.println("结尾追加成功");
            }
        }
    }

//（10） 在 HDFS 中，将文件从源路径移动到目的路径。
    /**
     * 移动文件/重命名
     * @param srcPath 源文件路径
     * @param dstPath 目标文件路径
     */
    public void mv(String srcPath,String dstPath)throws Exception{ 
        Path src=new Path(srcPath);
        Path dst=new Path(dstPath);
        if(!assertExists(src)) return;
        if(fs.rename(src, dst)){
            System.out.println("移动成功");
        }else{
            System.out.println("移动失败");
        }    
    }

    /**
     * 目录树节，仅调试用
     */
    class DirNode{
        public String name;
        public String path;
        public List<DirNode> children=new ArrayList<>();
        public DirNode(String name, String path) {
            this.name = name;
            this.path = path;
        }
    }
    /**
     * 获取hdfs文件树
     * @param path 递归起始路径，如'/''
     * @param maxDepth 最大递归深度
     * @param currentDepth 当前递归深度(内部调用默认传0)
     * @return 目录树节点
     */
    private DirNode getDirTree(String path,boolean showFiles) throws Exception { 
        return getDirTree(path, null, 0,showFiles);
    }
    private DirNode getDirTree(String path,Integer maxDepth,Integer currentDepth,boolean showFiles) throws Exception { 
        int effectiveMaxDepth = (maxDepth == null) ? 10 : maxDepth;
        Path currentPath = new Path(path);
        FileStatus status = fs.getFileStatus(currentPath);
        DirNode node = new DirNode(currentPath.getName().isEmpty() ? "/" : currentPath.getName(), path);

        if (!status.isDirectory() || currentDepth >= effectiveMaxDepth) {return node;}

        FileStatus[] subStatuses = fs.listStatus(currentPath);
        if (subStatuses != null) {
            for (FileStatus sub : subStatuses) {
                String subPath = sub.getPath().toUri().getPath();
                if (sub.isDirectory()) {
                    node.children.add(getDirTree(subPath, effectiveMaxDepth, currentDepth + 1,showFiles));
                }else if(showFiles && sub.isFile()){
                    node.children.add(new DirNode(sub.getPath().getName(), subPath));
                }
            }
        }
        return node;
    }
    private void printTree(DirNode node,String indent){
        System.out.println(indent + "└── " + node.name);
        for (DirNode child : node.children) {
            printTree(child, indent + "    ");
        }
    }
    public void showTree(String path,boolean showFiles)throws Exception{
        DirNode node = getDirTree(path,showFiles);
        printTree(node, "");
    }

    // 断言路径存在
    private boolean assertExists(Path path) throws IOException {
        if (!fs.exists(path)) {
            System.err.println(" [" + path + "] 不存在！");
            return false;
        }
        return true;
    }

    //断言路径不存在
    private boolean assertNotExists(Path path) throws IOException {
        if (fs.exists(path)) {
            System.err.println(" [" + path + "] 已存在！");
            return false;
        }
        return true;
    }
    public FileSystem getFs() {
        return this.fs;
    }
}
