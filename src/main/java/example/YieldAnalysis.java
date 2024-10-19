// 分析mfd_day_share_interest.csv文件，分析7日年化收益率和日均交易量的关系
package example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
// import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.text.DecimalFormat;

public class YieldAnalysis {

    public static class YieldMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> yieldMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 读取 mfd_day_share_interest.csv 文件
            // Path cacheFile = new Path("./Purchase/mfd_day_share_interest.csv");
            // BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()));

            Path stopWordsPath = new Path("hdfs:/lhd/input/mfd_day_share_interest.csv");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                reader.readLine(); // 跳过表头
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 3) {
                        yieldMap.put(fields[0], fields[2]); // 日期 -> 7日年化收益率
                    }
                }
                reader.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) {
                return;
            }

            String dateStr = fields[0];
            String[] amounts = fields[1].split(",");
            if (amounts.length < 2) {
                return;
            }

            String yieldStr = yieldMap.get(dateStr);
            if (yieldStr == null) {
                return;
            }

            double yield = Double.parseDouble(yieldStr);
            String range;
            if (yield >= 4 && yield < 5) {
                range = "4-5";
            } else if (yield >= 5 && yield < 6) {
                range = "5-6";
            } else if (yield >= 6 && yield < 7) {
                range = "6-7";
            } else {
                return;
            }

            context.write(new Text(range), new Text(amounts[0] + "," + amounts[1]));
        }
    }

    public static class YieldReducer extends Reducer<Text, Text, Text, Text> {
        private static final DecimalFormat df = new DecimalFormat("#");
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalInflow = 0;
            long totalOutflow = 0;
            int count = 0;

            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                long inflow = Long.parseLong(amounts[0]);
                long outflow = Long.parseLong(amounts[1]);

                totalInflow += inflow;
                totalOutflow += outflow;
                count++;
            }

            double avgInflow = (double) totalInflow / count;
            double avgOutflow = (double) totalOutflow / count;

            // 格式化输出为整数，不使用科学计数法
            String formattedAvgInflow = df.format(avgInflow);
            String formattedAvgOutflow = df.format(avgOutflow);

            context.write(key, new Text(formattedAvgInflow + "," + formattedAvgOutflow));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: YieldAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Yield Analysis");

        job.setJarByClass(YieldAnalysis.class);
        job.setMapperClass(YieldMapper.class);
        job.setReducerClass(YieldReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 添加缓存文件
        // job.addCacheFile(new Path("./Purchase/mfd_day_share_interest.csv").toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}