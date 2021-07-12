package com.amazonaws.lambda.mihai.textrinauroraexpopics.model;

/**
 * VO mapped with database table: TAB_SCANS
 * @author Mihai ADAM
 *
 */
public class Scan {
	
	private final String s3Key;//S3 Key: romexpo/2019.01.01_diverse/book-1_covf-1_pagn-120_typec-mag.jpg
	
	private String s3Bucket;
	private String scanS3URL;
	private String s3ObjArn;
	private String s3ObjEtag;
	private String scanFilePath;
	private String jsonMlFilePath;
	
	public Scan(String s3Key) {
		this.s3Key = s3Key;
	}

	public String getS3Bucket() {
		return s3Bucket;
	}

	public void setS3Bucket(String s3Bucket) {
		this.s3Bucket = s3Bucket;
	}

	public String getScanS3URL() {
		return scanS3URL;
	}

	public void setScanS3URL(String scanS3URI) {
		this.scanS3URL = scanS3URI;
	}

	public String getS3ObjArn() {
		return s3ObjArn;
	}

	public void setS3ObjArn(String s3ObjArn) {
		this.s3ObjArn = s3ObjArn;
	}

	public String getS3ObjEtag() {
		return s3ObjEtag;
	}

	public void setS3ObjEtag(String s3ObjEtag) {
		this.s3ObjEtag = s3ObjEtag;
	}

	public String getScanFilePath() {
		return scanFilePath;
	}

	public void setScanFilePath(String scanFilePath) {
		this.scanFilePath = scanFilePath;
	}

	public String getJsonMlFilePath() {
		return jsonMlFilePath;
	}

	public void setJsonMlFilePath(String jsonMlFilePath) {
		this.jsonMlFilePath = jsonMlFilePath;
	}

	public String getS3Key() {
		return s3Key;
	}
	
	
}
