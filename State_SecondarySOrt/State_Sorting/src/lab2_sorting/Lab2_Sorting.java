/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2_sorting;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author rajani
 */
public class Lab2_Sorting {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        try{
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf , "secondary sort");
        job.setJarByClass(Lab2_Sorting.class);
        job.setMapperClass(Lab2_Mapper.class);
        
        
        job.setReducerClass(Lab2_Reducer.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        
        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("sorting-driver");
        }catch(IOException | ClassNotFoundException ex){
            Logger.getLogger(Lab2_Sorting.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
