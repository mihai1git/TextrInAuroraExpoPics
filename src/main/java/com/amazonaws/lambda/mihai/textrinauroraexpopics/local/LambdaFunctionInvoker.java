package com.amazonaws.lambda.mihai.textrinauroraexpopics.local;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers.LambdaS3ToDbHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.RequestParametersEntity;
import com.amazonaws.services.s3.event.S3EventNotification.ResponseElementsEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import com.amazonaws.services.s3.event.S3EventNotification.UserIdentityEntity;

/**
 * local invoker for lambda that transfers data from File System to Database System; the handler runs as an ordinary class outside the AWS Cloud
 * works with a windows localhost directory structure, similar with S3 one
 * @author Mihai ADAM
 *
 */
public class LambdaFunctionInvoker {
    
    
    public static void main(String[] args) {
    	
    	AmazonS3 s3Client = null;
    	LambdaS3ToDbHandler handler = new LambdaS3ToDbHandler(s3Client);//just do not init cloud clients
    	
    	handler.setS3Dao(new DiskDao());
    	
    	// build notification event
    	S3BucketEntity s3Bucket = new S3BucketEntity("pics-repository-textract", new UserIdentityEntity("root"), "");
    	//S3ObjectEntity s3Obj = new S3ObjectEntity("romexpo/2019.01.01_diverse/201901011_visit-20_covf-1_pagn-1_co-kitchenshop;pentrugatit.jpg", new Long(123), "tag", "v1", "12469879491984");
    	S3ObjectEntity s3Obj = new S3ObjectEntity("romexpo/2020.03.01_diverse/202003011_visit-305_covf-1_pagn-1_co-m&c,business.json", new Long(123), "tag", "v1", "12469879491984");
    	S3Entity s3Entity = new S3Entity("local", s3Bucket, s3Obj, "local");
    	
    	S3EventNotificationRecord msg = new S3EventNotificationRecord("US-EAST-2", "S3", "S3", "2021-06-30T01:20", "v1", new RequestParametersEntity("localhost"), new ResponseElementsEntity("0", "0"), s3Entity, new UserIdentityEntity("root"));
    	List<S3EventNotificationRecord> s3Messages = new ArrayList<S3EventNotificationRecord>();
    	s3Messages.add(msg);
    	S3Event event = new S3Event(s3Messages);
    	
    	System.out.println("key: " + event.getRecords().get(0).getS3().getObject().getKey());
    	
    	Context mockContext = new LocalContext();
    	
    	//INVOKE LOCALLY
    	handler.handleRequest(event, mockContext);
    	
    }
}
