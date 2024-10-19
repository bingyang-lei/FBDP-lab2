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

public class DailyCapitalFlows {

    public static class DailyCapitalFlowsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (key.get() == 0 && value.toString().contains("user_id")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length < 10) {
                return;
            }

            String reportDate = fields[1];
            String totalPurchaseAmt = fields[4].isEmpty() ? "0" : fields[4];
            String totalRedeemAmt = fields[8].isEmpty() ? "0" : fields[8];

            context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
        }
    }

    public static class DailyCapitalFlowsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalInflow = 0;
            long totalOutflow = 0;

            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                long inflow = Long.parseLong(amounts[0]);
                long outflow = Long.parseLong(amounts[1]);

                totalInflow += inflow;
                totalOutflow += outflow;
            }

            context.write(key, new Text(totalInflow + "," + totalOutflow));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DailyCapitalFlows <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Capital Flows");

        job.setJarByClass(DailyCapitalFlows.class);
        job.setMapperClass(DailyCapitalFlowsMapper.class);
        job.setReducerClass(DailyCapitalFlowsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}