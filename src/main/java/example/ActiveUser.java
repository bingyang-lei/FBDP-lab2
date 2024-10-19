package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ActiveUser {

    public static class ActiveDaysMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (key.get() == 0 && value.toString().contains("user_id")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length < 9) {
                return;
            }

            String userId = fields[0];
            String directPurchaseAmt = fields[5];
            String totalRedeemAmt = fields[8];
            // String Value = "1";

            if ((!directPurchaseAmt.isEmpty() && Double.parseDouble(directPurchaseAmt) > 0) ||
                (!totalRedeemAmt.isEmpty() && Double.parseDouble(totalRedeemAmt) > 0)) {
                context.write(new Text(userId), new Text("1")); // 1 表示活跃
            }
        }
    }

    public static class ActiveDaysReducer extends Reducer<Text, Text, Text, Text> {
        Map<String, Integer> userActiveDays = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int activeDays = 0;
            for (Text value : values) {
                activeDays++;
            }
            userActiveDays.put(key.toString(), activeDays);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 按照值的大小排序,并输出
            // 将 HashMap 转换为 List
        List<Map.Entry<String, Integer>> list = new ArrayList<>(userActiveDays.entrySet());

        // 使用 Collections.sort 进行排序
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                int result = o2.getValue().compareTo(o1.getValue());
                if (result == 0) {
                    return o1.getKey().compareTo(o2.getKey()); // 如果活跃天数相同，按用户ID排序
                }
                return result;
            }
        });

        // 输出排序后的结果
        for (Map.Entry<String, Integer> entry : list) {
            context.write(new Text(entry.getKey()), new Text(Integer.toString(entry.getValue())));
        }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ActiveUser <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Active Days");

        job.setJarByClass(ActiveUser.class);
        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(ActiveDaysReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
