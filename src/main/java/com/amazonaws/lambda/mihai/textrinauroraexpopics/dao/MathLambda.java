package com.amazonaws.lambda.mihai.textrinauroraexpopics.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * class used to permute strings in the name of the company
 *
 */
public class MathLambda {
	
	/**
	 * 
	 * @param names the names of the company in a comma-separated value
	 * @return names of the company in more comma-separated values
	 */
	public static List<String> permute(String names) {
		List<String> result = new ArrayList<>();
				
		String[] compNames = names.split(",");
		
		int[] nums = new int[compNames.length];
		Arrays.setAll(nums, p -> p );		
	    List<List<Integer>> resultIdx = permute(nums);

	    
	    for (List<Integer> perm :  resultIdx) {
	    	StringBuilder compNamesPerm = new StringBuilder("'");
	    	
	    	for (Integer idx : perm) {
	    		
	    		compNamesPerm.append(compNames[idx]).append(",");
	    	}
	    	
	    	compNamesPerm.deleteCharAt(compNamesPerm.length()-1);
	    	compNamesPerm.append("'");
	    	
	    	result.add(compNamesPerm.toString());
	    }
	    	    
	    return result;
	}


	/**
	 * the basic algorithm for permutations, based on numbers
	 * @param nums
	 * @return
	 */
	private static List<List<Integer>> permute(int[] nums) {
	    List<List<Integer>> result = new ArrayList<>();
	    helper(0, nums, result);
	    return result;
	}
	 
	private static void helper(int start, int[] nums, List<List<Integer>> result){
	    if(start==nums.length-1){
	        ArrayList<Integer> list = new ArrayList<>();
	        for(int num: nums){
	            list.add(num);
	        }
	        result.add(list);
	        return;
	    }
	 
	    for(int i=start; i<nums.length; i++){
	        swap(nums, i, start);
	        helper(start+1, nums, result);
	        swap(nums, i, start);
	    }
	}
	 
	private  static void swap(int[] nums, int i, int j){
	    int temp = nums[i];
	    nums[i] = nums[j];
	    nums[j] = temp;
	}
}
