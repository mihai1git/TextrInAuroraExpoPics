package com.amazonaws.lambda.mihai.textrinauroraexpopics.local;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
 * mock for AWS Cloud Context object that in send to the handler
 * @author Mihai ADAM
 *
 */
public class LocalContext implements Context {

	@Override
	public String getAwsRequestId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLogGroupName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLogStreamName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFunctionName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFunctionVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInvokedFunctionArn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CognitoIdentity getIdentity() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientContext getClientContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRemainingTimeInMillis() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMemoryLimitInMB() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public LambdaLogger getLogger() {
		// TODO Auto-generated method stub
		
		return new LocalLogger();
	}

}
