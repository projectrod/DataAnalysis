package dataanalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapValuesByDate {

    public static class DateMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String dateTime = Bytes.toString(value.getValue(Bytes.toBytes("attributes"), Bytes.toBytes("Date")));
            byte[] badge = value.getValue(Bytes.toBytes("attributes"), Bytes.toBytes("Name"));
            String date = dateTime.split("T")[0];
            context.write(new ImmutableBytesWritable(Bytes.toBytes(date)), new ImmutableBytesWritable(badge));
        }
    }

    public static class DateReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable row, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> badgesCount = new HashMap<String, Integer>();
            for (ImmutableBytesWritable val : values) {
                String key = Bytes.toString(val.get());
                if (badgesCount.containsKey(key)) {
                    badgesCount.put(key, badgesCount.get(key) + 1);
                } else {
                    badgesCount.put(key, 1);
                }
            }

            Put put = new Put(row.get());
            for (String key : badgesCount.keySet()) {
                put.add(Bytes.toBytes("t1"), Bytes.toBytes(key), Bytes.toBytes(String.valueOf(badgesCount.get(key))));
            }
            context.write(new ImmutableBytesWritable(row.get()), put);
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Get Values by Date");
        job.setJarByClass(MapValuesByDate.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("Date"));
        scan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("Name"));
        TableMapReduceUtil.initTableMapperJob("badges", scan, DateMapper.class,
                ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("dates_badges", DateReducer.class, job);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
