package com.amazonaws.lambda.mihai.textrinauroraexpopics.dao;

import java.util.List;
import java.util.Map;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.ImageTextExtraction;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.Scan;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.textract.model.Block;

/**
 * Interface for the layer between the Lambda logic and the FileSystem storage; 
 * There are to implementations, one for each filesystem: AWS S3 and localhost windows NTFS
 * @author Mihai ADAM
 *
 */
public interface FileSystemDao {
	
	public String META_IMAGE_BUCKET = "image-bucket";
	public String IMAGE_BUCKET_DEFAULT_VALUE = "pics-repository";
	
	public String META_IMAGE_NAME = "image-key";

	public void writeToFileStorage(String s3ObjectBucket, String s3ObjectName, List<Block> blocks, String rawShortResult);
	
	public Scan getFileStorageInfo(String s3ObjectBucket, String s3ObjectName);
	
	public Map<String, String> getUserMetadata(String s3ObjectBucket, String s3ObjectName);
	
	public String getJsonFileName (String s3ObjectName);
	
	public ImageTextExtraction getDetectedTextFromFileStorage (String s3ObjectBucket, String s3ImageObjectName);
	
	public void setHandlerContext(Context handlerContext);
}
