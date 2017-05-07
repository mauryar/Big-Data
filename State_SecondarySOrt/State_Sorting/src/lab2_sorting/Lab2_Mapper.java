/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2_sorting;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author rajani
 */
public class Lab2_Mapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable>{

   
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    String values[] = value.toString().split("[|]");
    if((!values[0].equals("~")) && (!values[9].equals("~"))){
    CompositeKeyWritable cw = new CompositeKeyWritable(values[0], values[9]);
    
    try{
       context.write(cw, NullWritable.get());
       //context.write(cw, new Text(values[10]));
    }catch(IOException | InterruptedException ex){
        System.out.println("Error Message" + ex.getMessage());
    }
    }
    }
    
}
