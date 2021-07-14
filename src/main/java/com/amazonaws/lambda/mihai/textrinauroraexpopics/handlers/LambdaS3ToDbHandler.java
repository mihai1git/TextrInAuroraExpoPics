package com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.AuroraDao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.FileSystemDao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.S3Dao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.ImageTextExtraction;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.PageBuilder;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.Scan;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.Relationship;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;

/**
 * the Lambda function  that takes JSON Textract data from S3 and upload in Aurora database
 * runs in the cloud and on localhost
 * uses S3 attributes generated in a previous step
 * @author Mihai ADAM
 *
 */
public class LambdaS3ToDbHandler implements RequestHandler<S3Event, String> {
	   
	//POJO
	private FileSystemDao s3Dao = new S3Dao();
	private AuroraDao auroraDao = new AuroraDao();
        
    private Context handlerContext;

    /**
     * constructor used by AWS Cloud
     * this class runs also on localhost; see .local package
     */
    public LambdaS3ToDbHandler() {
    	AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
    	.withRegion(Regions.US_EAST_2)
		.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))//add X-Ray option
		.build();
    	
    	((S3Dao)s3Dao).setS3Client(s3Client);
    	
    }

    // Test purpose only.
    public LambdaS3ToDbHandler(AmazonS3 s3) {}

    /**
     * entry point in the Lambda function; this is invoked by the Cloud; can also be used on localhost
     *
     * @param event event from S3 sent by Cloud
     * @param context contains data set by Cloud from the running environment; mainly logging
     */
    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        this.handlerContext = context;
        s3Dao.setHandlerContext(context);
        auroraDao.setHandlerContext(context);
        String message = "START ";
        
        validateEvent(event);

        // Get the object from the event and show its content
        String s3ObjectBucket = event.getRecords().get(0).getS3().getBucket().getName();
        String s3ObjectName = event.getRecords().get(0).getS3().getObject().getKey();
        String s3ObjectNameUTF8 = null;
        
        context.getLogger().log(" bucket: " + s3ObjectBucket + " name (key): " + s3ObjectName);
        message += " bucket: " + s3ObjectBucket + " name (key): " + s3ObjectName;
        
        try {
        	
    		s3ObjectNameUTF8 = java.net.URLDecoder.decode(s3ObjectName, StandardCharsets.UTF_8.name());
        	context.getLogger().log(" bucket: " + s3ObjectBucket + " name UTF8 (key): " + s3ObjectNameUTF8);
        	
            
            Map<String, String> s3ObjectUserMetadata = s3Dao.getUserMetadata(s3ObjectBucket, s3ObjectNameUTF8);
            String s3ImageBucket = s3ObjectUserMetadata.get(FileSystemDao.META_IMAGE_BUCKET);
            String s3ImageName = s3ObjectUserMetadata.get(FileSystemDao.META_IMAGE_NAME);
            String s3ImageNameUTF8 = java.net.URLDecoder.decode(s3ImageName, StandardCharsets.UTF_8.name());
            
            context.getLogger().log("from METADATA image bucket: " + s3ImageBucket + " name (key): " + s3ImageName);
                        
        	
        	ImageTextExtraction s3CachedResult = s3Dao.getDetectedTextFromFileStorage(s3ObjectBucket, s3ObjectNameUTF8);
        	
        	List<Block> blocks = s3CachedResult.getExtractedTexts();
        	
        	handlerContext.getLogger().log("Text taken from S3 JSON");
            
            
         // Iterate through blocks and display detected text.
            
            handlerContext.getLogger().log("Detected lines and words: ");
    		message += " detected text: ";
    		//String rawShortResult = "";
            
            for (Block block : blocks) {
            	 
            	// see DisplayBlockInfo(block) for more detailed structure
            	
            	if (("LINE".equals(block.getBlockType()))) {
            		
            		handlerContext.getLogger().log("DETECTED: " + block.getText()
	            			+ " Confidence: " + ((block.getConfidence() == null) ? "" : block.getConfidence().toString())
	            			+ " Id : " + block.getId()
	            			+ " Type: " + block.getBlockType());
	            	
	            	message += " Id : " + block.getId() + " " + block.getText();
            		
                	//rawShortResult += block.getText() + "\n";
                }
            }
            
            // START DATABASE writes
            Scan scanS3Info = s3Dao.getFileStorageInfo(s3ImageBucket, s3ImageNameUTF8);
            
            scanS3Info.setScanFilePath(s3ImageBucket + "/" + s3ImageNameUTF8);
            scanS3Info.setJsonMlFilePath(s3ObjectBucket + "/" + s3ObjectNameUTF8);
                            
            PageBuilder pageData = new PageBuilder(s3ObjectNameUTF8);
            
            pageData.setS3Bucket(s3ObjectBucket);
            pageData.setScan(scanS3Info);
            
            auroraDao.writeToAurora (pageData, blocks);

            return message;
            
        }  catch (UnsupportedEncodingException e) {
    	    // not going to happen - value came from JDK's own StandardCharsets
    		throw new RuntimeException(e);
    		
    	} catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error while processing object %s from bucket %s. For S3, make sure they exist and"
                + " your bucket is in the same region as this function.", s3ObjectNameUTF8, s3ObjectBucket));
            throw e;
            
        } 
    }
    
    /**
     * validate event that came from AWS Cloud: only image files extensions
     * @param event
     * @throws RuntimeException if event not an image
     */
    private void validateEvent (S3Event event) {
    	String s3ObjectName = event.getRecords().get(0).getS3().getObject().getKey();
		
        //jump over non image files
        if (!validJsonName(s3ObjectName)) {
        	throw new RuntimeException("InvalidImageFormatException Unable to infer JSON type for key "+ s3ObjectName);
        }
    }
    
    /**
     * called from validateEvent
     * @param s3ObjectName
     * @return true if file name has image extension
     */
    private boolean validJsonName (String s3ObjectName) {
        Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(s3ObjectName);
        if (!matcher.matches()) {
        	return false;
        }
        String imageType = matcher.group(1);
        if (!"json".equalsIgnoreCase(imageType)) {
        	
        	return false;
        }
        return true;
    }
    

	public void setS3Dao(FileSystemDao s3Dao) {
		this.s3Dao = s3Dao;
	}

	public Context getHandlerContext() {
		return handlerContext;
	}

	public void setHandlerContext(Context handlerContext) {
		this.handlerContext = handlerContext;
	}
}