/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw5_4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author rajani
 */
public class HW5_4 {

    /**
     * @param args the command line arguments
     */
    
   
public static class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {

private MultipleOutputs<Text, NullWritable> mos = null;

protected void setup(Context context) {

mos = new MultipleOutputs(context);
}

protected void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
try {
String[] columns = value.toString().split("[|]");
String hours = columns[0].trim();
String raceRegion = columns[0] + "   " + columns[6] + "   " + columns[9];

if (hours.equals("<1")) {
mos.write("bins", raceRegion, NullWritable.get(), "<1");
}
if (hours.equals("1-4")) {
mos.write("bins", raceRegion, NullWritable.get(), "1-4");
}
if (hours.equals("5-9")) {
mos.write("bins", raceRegion, NullWritable.get(), "5-9");
}
if (hours.equals("10-14")) {
mos.write("bins", raceRegion, NullWritable.get(), "10-14");
}
if (hours.equals("15-19")) {
mos.write("bins", raceRegion, NullWritable.get(), "15-19");
}
if (hours.equals("20-24")) {
mos.write("bins", raceRegion, NullWritable.get(), "20-24");
}
if (hours.equals("25-29")) {
mos.write("bins", raceRegion, NullWritable.get(), "25-29");
}
if (hours.equals("30-34")) {
mos.write("bins", raceRegion, NullWritable.get(), "30-34");
}
if (hours.equals("35-39")) {
mos.write("bins", raceRegion, NullWritable.get(), "35-39");
}
if (hours.equals("40-44")) {
mos.write("bins", raceRegion, NullWritable.get(), "40-44");
}
if (hours.equals("45-49")) {
mos.write("bins", raceRegion, NullWritable.get(), "45-49");
}
if (hours.equals("50-54")) {
mos.write("bins", raceRegion, NullWritable.get(), "50-54");
}
if (hours.equals("55-59")) {
mos.write("bins", raceRegion, NullWritable.get(), "55-59");
}
if (hours.equals("60-64")) {
mos.write("bins", raceRegion, NullWritable.get(), "60-64");
}
if (hours.equals("65-69")) {
mos.write("bins", raceRegion, NullWritable.get(), "65-69");
}
if (hours.equals("70-74")) {
mos.write("bins", raceRegion, NullWritable.get(), "70-74");
}
if (hours.equals("75-79")) {
mos.write("bins", raceRegion, NullWritable.get(), "75-79");
}
if (hours.equals("80-84")) {
mos.write("bins", raceRegion, NullWritable.get(), "80-84");
}
if (hours.equals("85+")) {
mos.write("bins", raceRegion, NullWritable.get(), "85+");
}

} catch (Exception e) {
e.printStackTrace();
}
}

protected void cleanup(Context context) throws IOException, InterruptedException {

mos.close();
}
 }

public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Binning");
job.setJarByClass(HW5_4.class);

job.setNumReduceTasks(0);

//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(NullWritable.class);
job.setMapperClass(BinningMapper.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(NullWritable.class);
MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);

MultipleOutputs.setCountersEnabled(job, true);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

//job.setNumReduceTasks(8);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}    
    
}
