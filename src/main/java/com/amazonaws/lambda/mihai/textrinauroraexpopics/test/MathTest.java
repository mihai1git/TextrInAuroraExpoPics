package com.amazonaws.lambda.mihai.textrinauroraexpopics.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.MathLambda;

/**
 * 
 * @author Mihai ADAM
 *
 */
public class MathTest {

	public static void main(String[] args) {
		
		testMath();
	}
	
	private static void testMath() {
		List<String> compNames = new ArrayList<String>();
		compNames.add("marcu,group,dacia");
		
		for (String name : compNames) {
			
			List<String> namePerm = MathLambda.permute(name);
			
			System.out.println("perm for name: " + name + " : " + Arrays.toString(namePerm.toArray()));
		}
		
	}
}
