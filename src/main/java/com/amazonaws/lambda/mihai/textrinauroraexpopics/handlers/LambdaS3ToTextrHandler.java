package com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.dao.S3Dao;
import com.amazonaws.lambda.mihai.textrinauroraexpopics.model.ImageTextExtraction;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DetectDocumentTextRequest;
import com.amazonaws.services.textract.model.DetectDocumentTextResult;
import com.amazonaws.services.textract.model.Document;
import com.amazonaws.services.textract.model.Relationship;
import com.amazonaws.services.textract.model.S3Object;

public class LambdaS3ToTextrHandler implements RequestHandler<S3Event, String> {
	   
	//POJO
	private S3Dao s3Dao = new S3Dao();
    
    
    private AmazonTextract textrClient = AmazonTextractClientBuilder.standard().
    		withEndpointConfiguration(new EndpointConfiguration("https://textract.us-east-2.amazonaws.com", "us-east-2"))
    		//.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
    		.build();
    
    private Context handlerContext;

    public LambdaS3ToTextrHandler() {}

    // Test purpose only.
    LambdaS3ToTextrHandler(AmazonS3 s3) {
    	//s3Dao.setS3Client(s3);
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        this.handlerContext = context;
        s3Dao.setHandlerContext(context);
        String message = "START ";
        
        validateEvent(event);

        // Get the object from the event and show its content type
        String s3ObjectBucket = event.getRecords().get(0).getS3().getBucket().getName();
        String s3ObjectName = event.getRecords().get(0).getS3().getObject().getKey();
        String s3ObjectNameUTF8 = null;
        String targetS3ObjectBucket = "pics-repository-textract";
        
        context.getLogger().log(" bucket: " + s3ObjectBucket + " name (key): " + s3ObjectName);
        message += " bucket: " + s3ObjectBucket + " name (key): " + s3ObjectName;
        
        try {
        	
        	
        	try {
        		s3ObjectNameUTF8 = java.net.URLDecoder.decode(s3ObjectName, StandardCharsets.UTF_8.name());
            	context.getLogger().log(" bucket: " + s3ObjectBucket + " name UTF8 (key): " + s3ObjectNameUTF8);
                
        	} catch (UnsupportedEncodingException e) {
        	    // not going to happen - value came from JDK's own StandardCharsets
        		throw new RuntimeException(e);
        	}
            DetectDocumentTextRequest request = new DetectDocumentTextRequest()
                .withDocument(new Document().withS3Object(new S3Object().withName(s3ObjectNameUTF8).withBucket(s3ObjectBucket)));
                //.withSdkRequestTimeout(10000).withSdkClientExecutionTimeout(10000);
            
            DetectDocumentTextResult result = null;
            List<Block> blocks = null;
            
            result = textrClient.detectDocumentText(request);
            blocks = result.getBlocks();
            
            
         // Iterate through blocks and display detected text.
            
            handlerContext.getLogger().log("Detected lines and words: ");
    		message += " detected text: ";
    		String rawShortResult = "";
            
            for (Block block : blocks) {
            	 
            	// see DisplayBlockInfo(block) for more detailed structure
            	
            	if (("LINE".equals(block.getBlockType()))) {
            		
            		handlerContext.getLogger().log("DETECTED: " + block.getText()
	            			+ " Confidence: " + ((block.getConfidence() == null) ? "" : block.getConfidence().toString())
	            			+ " Id : " + block.getId()
	            			+ " Type: " + block.getBlockType());
	            	
	            	message += " Id : " + block.getId() + " " + block.getText();
            			
                //if (("LINE".equals(block.getBlockType()))) {
                	rawShortResult += block.getText() + "\n";
                }
            }
            
            s3Dao.writeToS3 (targetS3ObjectBucket, s3ObjectNameUTF8, blocks, rawShortResult);

            return message;
            
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error while processing object %s from bucket %s. For S3, make sure they exist and"
                + " your bucket is in the same region as this function.", s3ObjectNameUTF8, s3ObjectBucket));
            throw e;
            
        }
    }
    
    /**
     * validate event that came from AWS Cloud
     * @param event
     * @throws RuntimeException if event not an image
     */
    private void validateEvent (S3Event event) {
    	String s3ObjectName = event.getRecords().get(0).getS3().getObject().getKey();
		
        //jump over non image files: e.g. JSON files added by this lambda (TODO: in AWS Console: S3 event filter has an error now) 
        if (!validImageName(s3ObjectName)) {
        	throw new RuntimeException("InvalidImageFormatException Unable to infer image type for key "+ s3ObjectName);
        }
    }
    
    /**
     * called from validateEvent
     * @param s3ObjectName
     * @return true if file name has image extension
     */
    private boolean validImageName (String s3ObjectName) {
        Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(s3ObjectName);
        if (!matcher.matches()) {
        	return false;
        }
        String imageType = matcher.group(1);
        if (!("jpg".equalsIgnoreCase(imageType) 
        		|| "jpeg".equalsIgnoreCase(imageType) 
        		|| "png".equalsIgnoreCase(imageType)
        		|| "bmp".equalsIgnoreCase(imageType))) {
        	
        	return false;
        }
        return true;
    }
    
  //Displays information from a block returned by text detection and text analysis
    private void DisplayBlockInfo(Block block) {
        System.out.println("Block Id : " + block.getId());
        if (block.getText()!=null)
            System.out.println("    Detected text: " + block.getText());
        System.out.println("    Type: " + block.getBlockType());
        
        if (block.getBlockType().equals("PAGE") !=true) {
            System.out.println("    Confidence: " + block.getConfidence().toString());
        }
        if(block.getBlockType().equals("CELL"))
        {
            System.out.println("    Cell information:");
            System.out.println("        Column: " + block.getColumnIndex());
            System.out.println("        Row: " + block.getRowIndex());
            System.out.println("        Column span: " + block.getColumnSpan());
            System.out.println("        Row span: " + block.getRowSpan());

        }
        
        System.out.println("    Relationships");
        List<Relationship> relationships=block.getRelationships();
        if(relationships!=null) {
            for (Relationship relationship : relationships) {
                System.out.println("        Type: " + relationship.getType());
                System.out.println("        IDs: " + relationship.getIds().toString());
            }
        } else {
            System.out.println("        No related Blocks");
        }

        System.out.println("    Geometry");
        System.out.println("        Bounding Box: " + block.getGeometry().getBoundingBox().toString());
        System.out.println("        Polygon: " + block.getGeometry().getPolygon().toString());
        
        List<String> entityTypes = block.getEntityTypes();
        
        System.out.println("    Entity Types");
        if(entityTypes!=null) {
            for (String entityType : entityTypes) {
                System.out.println("        Entity Type: " + entityType);
            }
        } else {
            System.out.println("        No entity type");
        }
        if(block.getPage()!=null)
            System.out.println("    Page: " + block.getPage());            
        System.out.println();
    }
}