/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bloomfilter_lab6;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author rajani
 */
public class BloomFilter_Lab6 {

    /**
     * @param args the command line arguments
     */
    
    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable>{
        
        Funnel<Person> p = new Funnel<Person>(){
            @Override
            public void funnel(Person person, Sink into) {
                into.putInt(person.id).putString(person.firstName, Charsets.UTF_8)
                        .putString(person.lastName, Charsets.UTF_8)
                        //.putInt(person.birthYear)
                        ;
            }
        
    };
        
        private BloomFilter<Person> friends = BloomFilter.create(p, 500, 0.1);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
           // Person p1 = new Person(1999, "Female", "Acute Lymphocytic", 1593);
           // Person p2 = new Person(2008, "Female", "Acute Lymphocytic", 2147);
             Person p1 = new Person(1999, "Female", "Acute Lymphocytic");
             Person p2 = new Person(2008, "Female", "Acute Lymphocytic");
            
            ArrayList<Person> friendList = new ArrayList<Person>();
            friendList.add(p1);
            friendList.add(p2);
            
            for(Person pr : friendList){
                friends.put(pr);
            }
            
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try{
            String values[] = value.toString().split("[|]");
           // Person p = new Person(Integer.parseInt(values[0]), values[2], values[3], Integer.parseInt(values[8]));
        Person p = new Person(Integer.parseInt(values[0]), values[2], values[3]);
        
if((!values[0].equals("~")) && (!values[2].equals("~")) && (!values[3].equals("~")) && (!values[4].equals("Incidence")) && friends.mightContain(p)){
            //if(friends.mightContain(p)){
                context.write(value, NullWritable.get());
            }
            }catch(Exception e){
                
            }
        }
        
        
        
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BloomFilter");
        job.setJarByClass(BloomFilter_Lab6.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean success = job.waitForCompletion(true);
        
        System.out.println(success);
    }
    
}
