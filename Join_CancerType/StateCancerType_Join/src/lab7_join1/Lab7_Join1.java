/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab7_join1;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author rajani
 */
public class Lab7_Join1 extends Configured implements Tool{

    public static class JoinMapper1 extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] separatedInput = value.toString().split("[|]");
                String id = separatedInput[9];
                if(id == null){
                    return;
                }
                outKey.set(id);
                outValue.set("A" + separatedInput[9] + " " + separatedInput[4]);
                context.write(outKey, outValue);
            }catch(Exception e){
                
            }
        }
        
        
    }
    
      public static class JoinMapper2 extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try{
                 String[] separatedInput = value.toString().split("[|]");
                String id = separatedInput[10].trim();
                
                //String id = value.toString().split("[|]")[10].trim();
                
                if(id == null){
                    return;
                }
                if(!separatedInput[5].equals("Incidence")){
                outKey.set(id);
                outValue.set("B" + separatedInput[0] + " " + separatedInput[9]);
                context.write(outKey, outValue);
                }
            }catch(Exception e){
                
            }
        }
        
        
    }
    
      
    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        private static final Text EMPTY_TEXT = new Text();
        private Text tmp = new Text();
        
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        
        private String joinType = null;
        public void setup(Context context){
            joinType = context.getConfiguration().get("join.type");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();
            
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();
                
                if(tmp.charAt(0) == 'A'){
                    listA.add(new Text(tmp.toString().substring(1)));
                }else if(tmp.charAt(0) == 'B'){
                 listB.add(new Text(tmp.toString().substring(1)));
            }
        }
        
        executeJoinLogic(context);
        
    }
        
        private void executeJoinLogic(Context context){
            if(joinType.equalsIgnoreCase("leftouter")){
                for(Text A: listA){
                    if(!listB.isEmpty()){
                        for(Text B : listB){
                            try{
                                context.write(A, B);
                            }catch(Exception e){
                                
                            }
                        }
                    }else{
                        try{
                            context.write(A, EMPTY_TEXT);
                        }catch(Exception e){
                            
                        }
                    }
                }
            }else if(joinType.equalsIgnoreCase("rightouter")){
                for(Text B: listB){
                    if(!listA.isEmpty()){
                        for(Text A: listA){
                            try{
                                context.write(A, B);
                            }catch(Exception e){
                                
                            }
                        }
                    }else{
                        try{
                            context.write(EMPTY_TEXT, B);
                        }catch(Exception e){
                            
                        }
                    }
                }
            }else if(joinType.equalsIgnoreCase("fullouter")){
                if(!listA.isEmpty()){
                    for(Text A: listA){
                        if(listB.isEmpty()){
                            for(Text B: listB){
                                try{
                                    context.write(A, B);
                                }catch(Exception e){
                                    
                                }
                            }
                        }else{
                            try{
                                context.write(A, EMPTY_TEXT);
                            }catch(Exception e){
                                
                            }
                        }
                    }
                }else{
                    for(Text B: listB){
                        try{
                            context.write(EMPTY_TEXT, B);
                        }catch(Exception e){
                            
                        }
                    }
                }
            }else if(joinType.equalsIgnoreCase("anti")){
                if(listA.isEmpty() ^ listB.isEmpty()){
                    for(Text A: listA){
                        try{
                            context.write(A, EMPTY_TEXT);
                        }catch(Exception e){
                            
                        }
                    }
                    for(Text B : listB){
                        try{
                            context.write(EMPTY_TEXT, B);
                        }catch(Exception e){
                            
                        }
                    }
                }
            }
        }
      
    }
    public static void main(String[] args) {
        try{
            int res = ToolRunner.run(new Configuration(), new Lab7_Join1(), args);
        }catch(Exception e){
            
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReduceJoin");
        job.setJarByClass(Lab7_Join1.class);
        
        MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class, JoinMapper1.class);
        MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class, JoinMapper2.class);
        job.getConfiguration().set("join.type", "leftouter");
        
        job.setReducerClass(JoinReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(strings[2]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 2;
    }
    
}
