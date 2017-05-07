/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2_sorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author rajani
 */
public class GroupingComparator extends WritableComparator{
    protected GroupingComparator(){
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable cw1 = (CompositeKeyWritable)w1;
        CompositeKeyWritable cw2 = (CompositeKeyWritable)w2;

        return cw1.getDeptNo().compareTo(cw2.getDeptNo());        
    }
    
    
    
}
