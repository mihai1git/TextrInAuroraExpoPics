package com.amazonaws.lambda.mihai.textrinauroraexpopics.dao;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.ImageTextExtraction;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.Scan;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.textract.model.Block;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * layer between lambda logic and S3 filesystem; files are treated as objects
 * @author Mihai ADAM
 *
 */
public class S3Dao implements FileSystemDao {

	//client to be injected by CLOUD LAMBDA no-args constructor
    private AmazonS3 s3Client = null;
    
    //injected by Cloud with environment data; mainly logging
    private Context handlerContext;
    
    public void writeToFileStorage(String s3ObjectBucket, String s3ObjectName, List<Block> blocks, String rawShortResult)  {
    	this.writeToS3(s3ObjectBucket, s3ObjectName, blocks, rawShortResult);
    }
    
    
    /**
     * 
     * @param s3ObjectBucket folder name
     * @param s3ObjectName file name
     * @param jsonResult write to JSON file
     * @param rawShortResult write to TXT file
     * @param context for logging
     */
    public void writeToS3(String s3ObjectBucket, String s3ObjectName, List<Block> blocks, String rawShortResult)  {
    	
    	try {
    		 	
    		writeToS3Json(s3ObjectBucket, s3ObjectName, blocks);
    		
    		//writeToS3Txt(s3ObjectBucket, s3ObjectName, rawShortResult);
    		
            
    	}  catch (Exception ex) {
	    	ex.printStackTrace();
	    	throw new RuntimeException(ex);
	    }
    }
    
    /**
     * 
     * @param s3ObjectBucket
     * @param s3ObjectName
     * @param blocks
     * @throws IOException
     */
    private void writeToS3Json(String s3ObjectBucket, String s3ObjectName, List<Block> blocks) throws IOException, AmazonServiceException  {
    	
    	String jsonResult = getExtractionAsJson(s3ObjectName, blocks);
	 	
	 	 //context.getLogger().log("Extraction result: " + jsonResult);
	 	
       //for each image, write output to S3, in same folder
    // 1.JSON file
       ByteArrayOutputStream os = new ByteArrayOutputStream();
       os.write(jsonResult.getBytes());
       //ImageIO.write(resizedImage, imageType, os);
       InputStream is = new ByteArrayInputStream(os.toByteArray());
       // Set Content-Length and Content-Type
       ObjectMetadata meta = new ObjectMetadata();
       meta.setContentLength(os.size());
       meta.setContentLanguage("application/json");
       Map<String, String> userMetadata = new HashMap<String, String>();
       userMetadata.put(FileSystemDao.META_IMAGE_BUCKET, s3ObjectBucket);
       userMetadata.put(FileSystemDao.META_IMAGE_NAME, s3ObjectName);
       meta.setUserMetadata(userMetadata);


       // Uploading to S3 destination bucket
       String jsonFileName = getJsonFileName(s3ObjectName);
       handlerContext.getLogger().log("Writing to: " + s3ObjectBucket + "/" + jsonFileName);
       
       s3Client.putObject(s3ObjectBucket, jsonFileName, is, meta);
     
    }
    
    /**
     * 
     * @param s3ObjectBucket
     * @param s3ObjectName
     * @param rawShortResult
     * @throws IOException
     */
    private void writeToS3Txt(String s3ObjectBucket, String s3ObjectName, String rawShortResult) throws IOException, AmazonServiceException  {
       
     //2.raw TXT file
    	ByteArrayOutputStream os = new ByteArrayOutputStream();
       os.write(rawShortResult.getBytes());
       InputStream is = new ByteArrayInputStream(os.toByteArray());
       // Set Content-Length and Content-Type
       ObjectMetadata meta = new ObjectMetadata();
       meta.setContentLength(os.size());
       meta.setContentLanguage("text/plain");
       Map<String, String> userMetadata = new HashMap<String, String>();
       userMetadata.put(FileSystemDao.META_IMAGE_BUCKET, s3ObjectBucket);
       userMetadata.put(FileSystemDao.META_IMAGE_NAME, s3ObjectName);
       meta.setUserMetadata(userMetadata);

       // Uploading to S3 destination bucket
       String txtFileName = getTxtFileName(s3ObjectName);
       handlerContext.getLogger().log("Writing to: " + s3ObjectBucket + "/" + txtFileName);
       
       s3Client.putObject(s3ObjectBucket, txtFileName, is, meta);
    }
    

    /**
     * 
     * @param s3ObjectBucket
     * @param s3ObjectName
     */
    public Map<String, String> getUserMetadata(String s3ObjectBucket, String s3ObjectName) {
    	Map<String, String> userMetadata = s3Client.getObjectMetadata(s3ObjectBucket, s3ObjectName).getUserMetadata();
    	if (!userMetadata.containsKey(FileSystemDao.META_IMAGE_BUCKET)) {
    		userMetadata.put(FileSystemDao.META_IMAGE_BUCKET, FileSystemDao.IMAGE_BUCKET_DEFAULT_VALUE);
    	}
    	if (!userMetadata.containsKey(FileSystemDao.META_IMAGE_NAME)) {
    		userMetadata.put(FileSystemDao.META_IMAGE_NAME, getJpgFileName(s3ObjectName));
    	}
    	return userMetadata;
    }
    
    public Scan getFileStorageInfo(String s3ObjectBucket, String s3ObjectName) {
    	return this.getS3Info(s3ObjectBucket, s3ObjectName);
    }
    

    /**
     * 
     * @param s3ObjectBucket
     * @param s3ObjectName
     * @param context
     * @return
     */
    public Scan getS3Info(String s3ObjectBucket, String s3ObjectName) {
    	Scan scan = new Scan(s3ObjectName);
    	
    	scan.setS3Bucket(s3ObjectBucket);
    	//scan.setScanFilePath(s3ObjectName);
    	//scan.setJsonMlFilePath(getJsonFileName(s3ObjectName));
    
    	if (s3Client.doesObjectExist(s3ObjectBucket, s3ObjectName)) {
    		
    		URL url = s3Client.getUrl(s3ObjectBucket, s3ObjectName);
    		
    		handlerContext.getLogger().log("URL: " + url.toExternalForm());
    		
    		scan.setScanS3URL(url.toExternalForm());
    		        		        		
    		
    	} else {
    		throw new RuntimeException("S3 Object with bucket: " + s3ObjectBucket + " and key: " + s3ObjectName + " does not exits." );
    	}

    	return scan;
    }
    
    /**
     * extract file name without extension
     * @param s3ObjectName
     * @return
     */
    private String getNoExtFileName (String s3ObjectName) {
		 Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(s3ObjectName);
         if (!matcher.matches()) {
        	 System.out.println("Unable to infer image type for key " + s3ObjectName);
             throw new RuntimeException("Unable to infer image type for key "+ s3ObjectName);
         }
         String imageType = matcher.group(1);
         return s3ObjectName.substring(0, matcher.start(1));
    }
    
    /**
     * convert key from image name to json name
     * @param s3ObjectName
     * @return
     */
    public String getJsonFileName (String s3ObjectName) {
    	return getNoExtFileName(s3ObjectName) + "json";
    }

    
    /**
     * convert key from image name to txt name
     * @param s3ObjectName
     * @return
     */
    private String getTxtFileName (String s3ObjectName) {
    	return getNoExtFileName(s3ObjectName) + "txt";
    }
    
    /**
     * convert key from JSON name to initial image name (jpg file extension)
     * @param s3ObjectName
     * @return
     */
    public String getJpgFileName (String s3ObjectName) {
    	return getNoExtFileName(s3ObjectName) + "jpg";
    }
    
    /**
     * Jakson serializer used here
     * @param s3Path the s3 key
     * @param textractResult the result of the call to ML service Textraction
     * @return JSON format of the key and ML Textraction call
     */
    private String getExtractionAsJson (String s3Path, List<Block> textractResult) {
    	StringBuffer sb = new StringBuffer();
    	
    	ImageTextExtraction result = new ImageTextExtraction();
    	result.setExtractedTexts(textractResult);
    	result.setFileName(getFileName(s3Path));
    	result.setExibition(getExibitionName(s3Path));
    	
    	ObjectMapper om = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);
    	//ObjectWriter ow = om.writer().withDefaultPrettyPrinter();
		ObjectWriter ow = om.writer();
    	
    	try {
    		sb.append(ow.writeValueAsString(result));
    		
    	} catch (JsonProcessingException ex) {
    		ex.printStackTrace();
    	}
    	
    	return sb.toString();
    }
    

    
    /**
     * 
     * @param s3ObjectName
     * @return only the file name from the s3 key
     */
    private String getFileName (String s3ObjectName) {
    	
		String[] s3ObjectNameParts =  s3ObjectName.split("/");
		
        return s3ObjectNameParts[s3ObjectNameParts.length - 1];
   }
    
    /**
     * 
     * @param s3ObjectName
     * @return only the exibition name from the s3 key: first parent folder of the file
     */
    private String getExibitionName (String s3ObjectName) {
    	
		String[] s3ObjectNameParts =  s3ObjectName.split("/");
		
        return s3ObjectNameParts[1];
   }
    
    public ImageTextExtraction getDetectedTextFromFileStorage (String s3ObjectBucket, String s3ImageObjectName) {
    	return this.getDetectedTextFromS3(s3ObjectBucket, s3ImageObjectName);
    }
   
   /**
    *  
    * @param s3ObjectBucket
    * @param s3ImageObjectName
    * @return
    */
    public ImageTextExtraction getDetectedTextFromS3 (String s3ObjectBucket, String s3ImageObjectName) {
    	
    	String s3Obj = s3Client.getObjectAsString(s3ObjectBucket, getJsonFileName(s3ImageObjectName));
		
    	handlerContext.getLogger().log("s3Obj as string: " + s3Obj);

    	ImageTextExtraction result = getJsonAsObject(s3Obj);
    	
    	return result;
    }
    
    /**
     * 
     * @param jsonObject
     * @return
     */
    public ImageTextExtraction getJsonAsObject (String jsonObject) {
    	
    	ImageTextExtraction result = null;
    	
    	ObjectMapper om = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);
    	
    	om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    	om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    	om.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
    	
    	try {
    		result = om.readValue(jsonObject.getBytes(), ImageTextExtraction.class);
    		
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		throw new RuntimeException(ex);
    	}
    	
    	
    	return result;
    }

	public AmazonS3 getS3Client() {
		return s3Client;
	}

	public void setS3Client(AmazonS3 s3Client) {
		this.s3Client = s3Client;
	}

	public void setHandlerContext(Context handlerContext) {
		this.handlerContext = handlerContext;
	}
    
    
}
