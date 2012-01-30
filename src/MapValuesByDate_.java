
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountWordPairsHBase {

    public static class DateMapper extends TableMapper<ImmutableBytesWritable, Writable> {
        
        public void map(ImmutableBytesWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
            Put put = new Put();
            context.write(key, put);
        }
    }

    public static class DateReducer extends TableReducer{
        
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Get Values by Date");
        job.setJarByClass(CountWordPairsHBase.class);
        job.setMapperClass(DateMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
        job.setOutputFormatClass(TableOutputFormat.class);
        Scan scan = new Scan();
        scan.addColumns("attributes");
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob("badges", scan, DateMapper.class, 
                ImmutableBytesWritable.class, Writable.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
