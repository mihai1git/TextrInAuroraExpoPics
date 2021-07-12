package com.amazonaws.lambda.mihai.textrinauroraexpopics.test;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.AuroraDao;

/**
 * 
 * @author Mihai ADAM
 *
 */
public class AuroraDaoTest {

	public static  void main(String[] args) {
		
		AuroraDao adao = new AuroraDao();
		String company = "romexpo,group,expo";
		System.out.println("getQueryToCheckLikeCompany: " + adao.getQueryToCheckLikeCompany(company));
		System.out.println("getQueryToCheckEqualCompany: " + adao.getQueryToCheckEqualCompany(company));
		
		company = "romexpo";
		System.out.println("getQueryToCheckLikeCompany: " + adao.getQueryToCheckLikeCompany(company));
		System.out.println("getQueryToCheckEqualCompany: " + adao.getQueryToCheckEqualCompany(company));
	}
}
