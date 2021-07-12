package com.amazonaws.lambda.mihai.textrinauroraexpopics.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.management.RuntimeErrorException;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * from SQS, lambda is invoked synchronously; using this, the destination is invoked asynchronously 
 * @author Mihai ADAM
 *
 */
public class LambdaSQSHelperHandler implements RequestStreamHandler {
	
	 // Jackson JSON mapper
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private Context handlerContext;

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
    	
    	context.getLogger().log("START handleRequest");
    	this.handlerContext = context;
    	String function = System.getenv("async_destination");
    	String response = "Response after invokeAsynchronously destination: " + function;
                
        try {
            JsonNode eventNode = OBJECT_MAPPER.readTree(input);  // root node
            String eventString = OBJECT_MAPPER.setSerializationInclusion(Include.NON_NULL).writer().writeValueAsString(eventNode);
            
            context.getLogger().log("Input json: " + eventString);
            
            int batchMessage = 0;
            String batchMessagePath = "/Records/" + batchMessage;
            
            while (!eventNode.at(batchMessagePath).isMissingNode()) {
            	
                String body = eventNode.at(batchMessagePath + "/body").asText();
                String messageId = eventNode.at(batchMessagePath + "/messageId").asText();
                context.getLogger().log("messageId: " + messageId + " body: " + body);
                
                invokeAsynchronously(function, body);
                
                response += " for sqs message: " + messageId;
                batchMessagePath = "/Records/" + ++batchMessage;
            }
            
            
        } catch (Exception ex) {
        	context.getLogger().log("exception: " + ex);
        	throw ex;
        }
        
        context.getLogger().log("response: " + response);        
        
        OBJECT_MAPPER.writeValue(output, response);
    }
    
    /**
     * from SQS, lambda is invoked synchronously
     * using this, the destination @param functionName is invoked asynchronously with JSON request @param functionInput
     * @param functionName
     * @param functionInput
     */
    private void invokeAsynchronously(String functionName, String functionInput) {
    	
    	AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
        InvokeRequest req = new InvokeRequest()
            .withFunctionName(functionName)
            .withPayload(ByteBuffer.wrap(functionInput.getBytes()));

        Future<InvokeResult> future_res = lambda.invokeAsync(req);
        
        handlerContext.getLogger().log("Waiting for future");
        while (future_res.isDone() == false) {
        	handlerContext.getLogger().log(".");
            try {
                Thread.sleep(1000);
                
            } catch (InterruptedException e) {
            	handlerContext.getLogger().log("\nThread.sleep() was interrupted!");
                throw new RuntimeException(e);
            }
        }

        try {
            InvokeResult res = future_res.get();
            handlerContext.getLogger().log(functionName + " request ID: " + res.getSdkResponseMetadata().getRequestId() + " status code: " + res.getStatusCode() + " error: " + res.getFunctionError());
            
            if (res.getStatusCode() == 200 && res.getFunctionError() == null) {
            	
            	handlerContext.getLogger().log("\nLambda function returned:");
                ByteBuffer response_payload = res.getPayload();
                handlerContext.getLogger().log(new String(response_payload.array()));
                
            } else {
            	
            	String errorMsg = "Received a non-OK response from AWS: status code: " + res.getStatusCode() + " error: " + res.getFunctionError();
           	 	handlerContext.getLogger().log(errorMsg);
           	 
            	if (res.getFunctionError() != null) {
                	handlerContext.getLogger().log("\nLambda function returned ERROR:");
                    ByteBuffer response_payload = res.getPayload();
                    handlerContext.getLogger().log(new String(response_payload.array()));
            	}
            	

            	//throw new RuntimeException(errorMsg);
            }
            
        } catch (InterruptedException | ExecutionException e) {
        	handlerContext.getLogger().log(e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
