package com.amazonaws.lambda.mihai.textrinauroraexpopics.local;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.FileSystemDao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.S3Dao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.ImageTextExtraction;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.Scan;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.textract.model.Block;

/**
 *  layer between lambda logic and Windows filesystem
 * @author Mihai ADAM
 *
 */
public class DiskDao implements FileSystemDao {
	
	
	private static final String fileStorageRoot = "D:\\work\\amazon\\SCAN_ML_TEXTRACT\\S3FileSystem\\";
	
    private Context handlerContext;
 
	@Override
	public void writeToFileStorage(String s3ObjectBucket, String s3ObjectName, List<Block> blocks, String rawShortResult) {
		
		handlerContext.getLogger().log("JUMP OVER WRITE !");
	}

	@Override
	public Scan getFileStorageInfo(String s3ObjectBucket, String s3ObjectName) {
    	Scan scan = new Scan(s3ObjectName);
    	
    	scan.setS3Bucket(s3ObjectBucket);
    	
    	if (doesObjectExist(s3ObjectBucket, s3ObjectName)) {
    		
    		String url = getUrl(s3ObjectBucket, s3ObjectName);
    		
    		handlerContext.getLogger().log("URL: " + url);
    		
    		scan.setScanS3URL(url);
    		        		        		
    		
    	} else {
    		throw new RuntimeException("S3 Object with bucket: " + s3ObjectBucket + " and key: " + s3ObjectName + " does not exits." );
    	}
    	
		return scan;
	}
	
	private boolean doesObjectExist (String s3ObjectBucket, String s3ObjectName) {
		
	
		String filePathString = fileStorageRoot + s3ObjectBucket + "\\" + s3ObjectName;
		
		handlerContext.getLogger().log("filePathString: " + filePathString);
		
		Path path = Paths.get(filePathString);
		
		return Files.exists(path);
	}
	
	private String getUrl (String s3ObjectBucket, String s3ObjectName) {
		String url = "http:/localhost/textract/";
		
		url += s3ObjectBucket + "/" + s3ObjectName;
		
		return url;
	}
	
	@Override
	public Map<String, String> getUserMetadata(String s3ObjectBucket, String s3ObjectName) {
		
		Map<String, String> userMetadata = new HashMap<String, String>();
		userMetadata.put(FileSystemDao.META_IMAGE_BUCKET, FileSystemDao.IMAGE_BUCKET_DEFAULT_VALUE);
		userMetadata.put(FileSystemDao.META_IMAGE_NAME, (new S3Dao()).getJpgFileName(s3ObjectName));	
		
		return userMetadata;
	}

	@Override
	public String getJsonFileName(String s3ObjectName) {
		return (new S3Dao()).getJsonFileName(s3ObjectName);
	}

	@Override
	public ImageTextExtraction getDetectedTextFromFileStorage(String s3ObjectBucket, String s3ImageObjectName) {
		
		ImageTextExtraction result = null;
		String jsonFileName = (new S3Dao()).getJsonFileName(s3ImageObjectName);
		
		if (!doesObjectExist(s3ObjectBucket, jsonFileName)) {
			throw new RuntimeException("File " + s3ObjectBucket + "\\" + jsonFileName + " does not exists !");
		}
		
		String filePathString = fileStorageRoot + s3ObjectBucket + "\\" + jsonFileName;
			
		try {
	
			Path path = Paths.get(filePathString);			
			InputStream inputStream = Files.newInputStream(path);
			String s3Obj = readFromInputStream(inputStream);
			
	    	handlerContext.getLogger().log("s3Obj as string: " + s3Obj);

	    	result = (new S3Dao()).getJsonAsObject(s3Obj);
	    	
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		 
		return result;
	}
	
	private String readFromInputStream(InputStream inputStream) throws IOException {
		
	    StringBuilder resultStringBuilder = new StringBuilder();
	    try (BufferedReader br
	      = new BufferedReader(new InputStreamReader(inputStream))) {
	        String line;
	        while ((line = br.readLine()) != null) {
	            resultStringBuilder.append(line).append("\n");
	        }
	    }
	    
	  return resultStringBuilder.toString();
	}

	@Override
	public void setHandlerContext(Context handlerContext) {
		this.handlerContext = handlerContext;
	}

}
