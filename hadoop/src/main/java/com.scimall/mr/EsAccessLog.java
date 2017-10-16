package com.scimall.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * Created by Zouyy on 2017/8/30.
 */
public class EsAccessLog {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //配置es ip信息
        conf.set(ConfigurationOptions.ES_NODES, "10.10.10.24");
        //ES port
        conf.set(ConfigurationOptions.ES_PORT, "9200");
        //ES 对应index和type
        conf.set(ConfigurationOptions.ES_RESOURCE, "operation_log/operation_log/");
        //设置输出各式为json格式
        conf.set(ConfigurationOptions.ES_OUTPUT_JSON, "false");

        Job job = Job.getInstance(conf, "esAccessLog");
        job.setJarByClass(EsAccessLog.class);
        job.setMapperClass(EsAccessLogMapper.class);

        job.setInputFormatClass(EsInputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String de = df.format(date);

        FileOutputFormat.setOutputPath(job,new Path("hadoop/es_input_test02/"+de));

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class EsAccessLogMapper extends Mapper<Object, Object, NullWritable, Text> {

        @Override
        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

            MapWritable doc = (MapWritable) value;
            StringBuilder builder = new StringBuilder();
            Set<Map.Entry<Writable, Writable>> entries = doc.entrySet();
            if (!entries.isEmpty()) {
                for (Map.Entry<Writable, Writable> entry : entries) {
                    builder.append(entry.getKey() + ":" + entry.getValue() + "\t");
                }
                Text text = new Text();
                if (builder.length() > 0) {
                    text.set(builder.toString().trim());
                    context.write(NullWritable.get(), text);
                }
            }

        }
    }
}
