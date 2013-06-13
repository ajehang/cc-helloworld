package org.commoncrawl.tutorial;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccessLogMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    String logEntryPattern = "([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\] \"([^\"]*)\"" + " ([^ ]*) ([^ ]*).*";

Pattern p = Pattern.compile(logEntryPattern);

@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
    String line = ((Text) value).toString();
    Matcher matcher = p.matcher(line);
    if (matcher.matches()) {
        String ip = matcher.group(1);
        context.write(new Text(ip), new IntWritable(1));
      }
}
}