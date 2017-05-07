/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_q5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author rajani
 */
public class SortComparator extends WritableComparator{
   
    protected SortComparator() {
		super(DoubleWritable.class, true);
	}

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable key1 = (DoubleWritable) a;
		DoubleWritable key2 = (DoubleWritable) b;

		//  sorting in descending order
		int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
		return result;
    }
    
    
    
} 
