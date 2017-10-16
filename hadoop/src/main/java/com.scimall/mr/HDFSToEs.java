package com.scimall.mr;


import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

/**
 * Created by Zouyy on 2017/9/7.
 */
public class HDFSToEs {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //配置es ip信息
        conf.set(ConfigurationOptions.ES_NODES, "10.10.10.24");
        //ES port
        conf.set(ConfigurationOptions.ES_PORT, "9200");
        //ES 对应index和type
        conf.set(ConfigurationOptions.ES_RESOURCE, "test_log/test_log");
        //设置输出各式为json格式
        conf.set(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Job job = Job.getInstance(conf, HDFSToEs.class.getSimpleName());

        job.setJarByClass(HDFSToEs.class);
        job.setMapperClass(MyMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);


        FileInputFormat.addInputPath(job,new Path(args[0]));
        //FileOutputFormat.setOutputPath(job,new Path("hadoop/output"));

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MyMapper extends Mapper<Object,Text,NullWritable,BytesWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength()>0){
                byte[] lines = value.toString().trim().getBytes();
                BytesWritable byteWritable = new BytesWritable(lines);
                context.write(NullWritable.get(),byteWritable);
            }
        }
    }
}
