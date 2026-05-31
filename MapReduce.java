import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;




public class MapReduce {
    // =========================================================================
    // 任务一：文件合并与去重 (DistinctTask)
    // =========================================================================
    public static class DistinctTask {
        public static class DistinctMapper extends Mapper<LongWritable, Text, Text, Text> {
            private final Text outValue = new Text("");
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().trim();
                if (!line.isEmpty()) {
                    // 以整行文本作为 key 发送，利用 Shuffle 阶段的自动按 key 聚合去重
                    context.write(new Text(line), outValue);
                }
            }
        }
        public static class DistinctReducer extends Reducer<Text, Text, Text, Text> {
            private final Text outValue = new Text("");
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                // 相同的 key（文本行）只会进入一次 reduce 方法，直接输出 key 即可
                context.write(key, outValue);
            }
        }
        public static void runJob(String inputPath, String outputPath) throws Exception {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "Merge and Distinct");
            job.setJarByClass(MapReduce.class);
            job.setMapperClass(DistinctMapper.class);
            job.setReducerClass(DistinctReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.out.println(">>> 任务一（去重）开始运行...");
            if (job.waitForCompletion(true)) {
                System.out.println(">>> 任务一运行成功！输出路径: " + outputPath);
            }
        }
    }

    // =========================================================================
    // 任务二：全局排序与位次标记 (SortTask)
    // =========================================================================
    public static class SortTask {
        public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
            private final IntWritable outKey = new IntWritable();
            private final IntWritable outValue = new IntWritable(1);
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().trim();
                if (!line.isEmpty()) {
                    // 转为整数以防按字符串字典序排序（12 排在 4 前面）
                    outKey.set(Integer.parseInt(line));
                    context.write(outKey, outValue);
                }
            }
        }
        public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
            // 定义一个 reduce 进程内的全局计数器进行位次标记
            // 注意：此方法仅在单 Reducer 情况下能保证绝对的 1,2,3... 全局位次
            private static int lineNum = 1;
            @Override
            protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                // 考虑数字重复出现的情况，用循环处理
                for (IntWritable val : values) {
                    context.write(new IntWritable(lineNum), key);
                    lineNum++;
                }
            }
        }
        public static void runJob(String inputPath, String outputPath) throws Exception {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "Global Sort");
            job.setJarByClass(MapReduce.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            // 强制设置 Reducer 数量为 1，确保全局有序和正确的行号计数
            job.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.out.println(">>> 任务二（排序）开始运行...");
            if (job.waitForCompletion(true)) {
                System.out.println(">>> 任务二运行成功！输出路径: " + outputPath);
            }
        }
    }

    // =========================================================================
    // 任务三：单表自连接挖掘祖孙关系 (RelationTask)
    // =========================================================================
    public static class RelationTask {
        public static class RelationMapper extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().trim();
                // 过滤表头或者空行
                if (line.isEmpty() || line.startsWith("child")) {
                    return;
                }
                // 兼容空格或制表符切分
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    String child = parts[0];
                    String parent = parts[1];
                    // 1. 作为“左表（子-亲表）”，以亲(parent)为Key，标记value为儿女(+child)
                    context.write(new Text(parent), new Text("1+" + child));                    
                    // 2. 作为“右表（亲-祖表）”，以子(child)为Key，标记value为父母(-parent)
                    context.write(new Text(child), new Text("2+" + parent));
                }
            }
        }
        public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                List<String> grandchildList = new ArrayList<>();
                List<String> grandparentList = new ArrayList<>();
                // 根据标签将数据分流到不同的集合中
                for (Text val : values) {
                    String str = val.toString();
                    if (str.startsWith("1+")) {
                        grandchildList.add(str.substring(2)); // 提取出孙子辈姓名
                    } else if (str.startsWith("2+")) {
                        grandparentList.add(str.substring(2)); // 提取出爷爷辈姓名
                    }
                }
                // 求两个集合的笛卡尔积，输出祖孙关系
                if (!grandchildList.isEmpty() && !grandparentList.isEmpty()) {
                    for (String grandchild : grandchildList) {
                        for (String grandparent : grandparentList) {
                            context.write(new Text(grandchild), new Text(grandparent));
                        }
                    }
                }
            }
        }

        public static void runJob(String inputPath, String outputPath) throws Exception {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "Grandchild-Grandparent Relation Mining");
            job.setJarByClass(MapReduce.class);
            job.setMapperClass(RelationMapper.class);
            job.setReducerClass(RelationReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.out.println(">>> 任务三（关系挖掘）开始运行...");            
            // 输出前先打印个表头以便辨识（MapReduce 默认不输出表头，可依据实验报告选填）
            // 如果实验严格要求输出纯数据，此步骤可在 HDFSUtil 下载后手动或用代码追加
            if (job.waitForCompletion(true)) {
                System.out.println(">>> 任务三运行成功！输出路径: " + outputPath);
            }
        }
    }
    public static void main(String[] args) throws Exception { 
        HdfsUtil dfs = new HdfsUtil();
        dfs.uploadFile("./mapreduce/file_A.txt", "/file_A.txt", false);
        dfs.uploadFile("./mapreduce/file_B.txt", "/file_B.txt", false);
        dfs.uploadFile("./mapreduce/sort_1.txt", "/sort_1.txt", false);
        dfs.uploadFile("./mapreduce/sort_2.txt", "/sort_2.txt", false);
        dfs.uploadFile("./mapreduce/sort_3.txt", "/sort_3.txt", false);
        dfs.uploadFile("./mapreduce/child_parent.txt", "/child_parent.txt", false);
        
        // 1. 任务一：文件合并与去重
        // 多个输入路径用逗号隔开，或者直接指定输入目录
        String input1 = "/file_A.txt,/file_B.txt"; 
        String output1 = "/output_dedup";
        DistinctTask.runJob(input1, output1);

        // 2. 任务二：全局排序与位次标记
        String input2 = "/sort_1.txt,/sort_2.txt,/sort_3.txt";
        String output2 = "/output_sort";
        SortTask.runJob(input2, output2);

        // 3. 任务三：单表自连接挖掘祖孙关系
        String input3 = "/child_parent.txt";
        String output3 = "/output_relation";
        RelationTask.runJob(input3, output3);

        dfs.downloadFile("/output_dedup/part-r-00000", "./mapreduce");
        dfs.downloadFile("/output_sort/part-r-00000", "./mapreduce");
        dfs.downloadFile("/output_relation/part-r-00000", "./mapreduce");





    }



}
