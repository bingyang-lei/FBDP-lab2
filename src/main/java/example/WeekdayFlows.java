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
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.DayOfWeek;

public class WeekdayFlows {

    public static class WeekdayFlowsMapper extends Mapper<LongWritable, Text, Text, Text> {
        // private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

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
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd"); // 定义日期格式

            // 将字符串转换为LocalDate
            LocalDate date = LocalDate.parse(dateStr, formatter);

            // 获取星期数
            DayOfWeek dayOfWeek = date.getDayOfWeek();
            String weekname = dayOfWeek.toString();
            context.write(new Text(weekname), new Text(amounts[0] + "," + amounts[1]));
            
        }
    }

    public static class WeekdayFlowsReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Long[]> weekdaySums = new HashMap<>();
        private Map<String, Integer> weekdayCounts = new HashMap<>();

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

            weekdaySums.put(key.toString(), new Long[]{totalInflow, totalOutflow});
            weekdayCounts.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Double[]> weekdayAverages = new HashMap<>();

            for (Map.Entry<String, Long[]> entry : weekdaySums.entrySet()) {
                String weekday = entry.getKey();
                Long[] sums = entry.getValue();
                int count = weekdayCounts.get(weekday);

                double avgInflow = (double) sums[0] / count;
                double avgOutflow = (double) sums[1] / count;

                weekdayAverages.put(weekday, new Double[]{avgInflow, avgOutflow});
            }

            weekdayAverages.entrySet().stream()
                    .sorted((e1, e2) -> Double.compare(e2.getValue()[0], e1.getValue()[0]))
                    .forEachOrdered(entry -> {
                        try {
                            context.write(new Text(entry.getKey()), new Text(entry.getValue()[0] + "," + entry.getValue()[1]));
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeekdayFlows <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekday Capital Flows");

        job.setJarByClass(WeekdayFlows.class);
        job.setMapperClass(WeekdayFlowsMapper.class);
        job.setReducerClass(WeekdayFlowsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
