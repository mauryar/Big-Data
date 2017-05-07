/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mortalitybyyear;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class MortalityByYear {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avgMortality");
        job.setJarByClass(MortalityByYear.class);
        //job.setNumReduceTasks(0);
        job.setMapperClass(Averagemapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountAverageTuple.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(CountAverageTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
        public static class Averagemapper extends Mapper<Object, Text, Text, CountAverageTuple> {

        CountAverageTuple outCountAverage = new CountAverageTuple();
        
        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            try{    
                String row[] = value.toString().split("[|]");
                String stockDate = row[10];
                //String stockDate = row[0];
                if((row[5].equals("Mortality")) && (!row[4].equals("~"))){
                outCountAverage.setAverage(Double.parseDouble(row[4]));
                
                outCountAverage.setCount(1);
                
                
                context.write(new Text(stockDate), outCountAverage);
                //context.write(new Text(stockDate), new Text(stockDate));
                }
            }catch(Exception e){
                
            }
        }

       
    }
        
        public static class AverageReducer extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple> {

            private CountAverageTuple result = new CountAverageTuple();
            

        @Override
        protected void reduce(Text key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
             
            double sum = 0;
            double count = 0;
            
            for (CountAverageTuple val : values){
                sum += val.getCount()*val.getAverage();
                count += val.getCount();
            }
            
            result.setCount(count);
            result.setAverage(sum/count);
            
            context.write(key, result);
            
        
        }

        
        }
}
