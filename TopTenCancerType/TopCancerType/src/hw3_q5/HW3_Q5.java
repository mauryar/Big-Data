/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_q5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
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
public class HW3_Q5 {

    /**
     * @param args the command line arguments
     */
    
    public static class Map1 extends Mapper<Object, Object, Object, Object>{
        
        public void map(Object key, Object value, Context context){
            String row[] = value.toString().split("[|]");
            if((!row[5].equals("Incidence")) && (!row[7].equals("All Cancer Sites Combined")) && (!row[4].equals("~"))){
            String cancerType = row[7];
            String patientMortality = row[4];
            
            try {
               DoubleWritable mortality = new DoubleWritable(Double.parseDouble(patientMortality));
               
               context.write(new Text(cancerType), mortality);
            } catch (Exception e) {
                
            }
        }      
        }
        
    }
    
    public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        
        private DoubleWritable totalSales = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            int count = 0;
            for(DoubleWritable val: values){
                sum += val.get();
                count++;
            }
            totalSales.set(sum/count);
            context.write(key, totalSales);
        }
    }
    
    public static class Map2
            extends Mapper<LongWritable, Text, DoubleWritable, Text>{
        
        public void map(LongWritable key, Text value, Context context){
        String[] row = (value.toString()).split("\\t");
        Text employeeId = new Text(row[0]);
        String salesAmount = row[1].trim();
        
            try {
                DoubleWritable count = new DoubleWritable(Double.parseDouble(salesAmount));
                 context.write(count, employeeId);
            } catch (Exception e) {
                
            }
            
        }
    }
    
    // sort in descending , make rating as key and movie id as value
    
    public static class Reduce2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
         int i =0;
        public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
       
            for(Text val : value){
                if(i<10){
                context.write(val, key);
                i++;
                }else{
                    return;
                }
            }
        
        }
        
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "chainingHw");
        job1.setJarByClass(HW3_Q5.class);
        job1.setMapperClass(Map1.class);
        //job1.setMapOutputKeyClass(Text.class);
        //job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        boolean complete = job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "chainingH");
        if(complete){
        job2.setJarByClass(HW3_Q5.class);
        job2.setMapperClass(Map2.class);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setSortComparatorClass(SortComparator.class);
        
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
