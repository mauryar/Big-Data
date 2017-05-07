/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab3_empchaining;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author rajani
 */
public class Lab3_EmpChaining {

    /**
     * @param args the command line arguments
     */
    
    
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        public void map(LongWritable key, Text value, Context context){
            String row[] = value.toString().split("[|]");
            if((!row[0].equals("~")) && (!row[3].equals("~"))){
            String employeeId = row[0];
            String salesAmount = row[3].trim();
            
            try {
               IntWritable sales = new IntWritable(Integer.parseInt(salesAmount));
               context.write(new Text(employeeId), sales);
            } catch (Exception e) {
                
            }
            }
                    
        }
        
    }
    
    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        private IntWritable totalSales = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            totalSales.set(sum);
            context.write(key, totalSales);
        }
    }
    
    public static class Map2
            extends Mapper<LongWritable, Text, IntWritable, Text>{
        
        public void map(LongWritable key, Text value, Context context){
        String[] row = (value.toString()).split("\\t");
        Text employeeId = new Text(row[0]);
        String salesAmount = row[1].trim();
        
            try {
                IntWritable count = new IntWritable(Integer.parseInt(salesAmount));
                 context.write(count, employeeId);
            } catch (Exception e) {
                
            }
            
        }
    }
    
    
    
    public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable>{
        
        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
        
            for(Text val : value){
                context.write(val, key);
            }
        
        }
        
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "chaining");
        job1.setJarByClass(Lab3_EmpChaining.class);
        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        boolean complete = job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "chaining");
        if(complete){
        job2.setJarByClass(Lab3_EmpChaining.class);
        job2.setMapperClass(Map2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
    
    
}
