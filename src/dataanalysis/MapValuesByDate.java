package dataanalysis;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class MapValuesByDate {

    public static class DateMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
        
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String dateTime = Bytes.toString(value.getValue(Bytes.toBytes("attributes"), Bytes.toBytes("Date")));
            String date = dateTime.split("T")[0];
			context.write(new ImmutableBytesWritable(Bytes.toBytes(date)), new IntWritable(1));
        }
        
    }
    
    public static class DateReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

    	public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int i = 0;
    		for (IntWritable val : values) {
    			i += val.get();
    		}
    		Put put = new Put(Bytes.toBytes(key.toString()));
    		put.add(Bytes.toBytes("t1"), Bytes.toBytes("count"), Bytes.toBytes(i));

    		context.write(null, put);
    	}
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Get Values by Date");
        job.setJarByClass(MapValuesByDate.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("Date"));
        TableMapReduceUtil.initTableMapperJob("badges", scan, DateMapper.class, 
        		ImmutableBytesWritable.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("test", DateReducer.class, job);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
