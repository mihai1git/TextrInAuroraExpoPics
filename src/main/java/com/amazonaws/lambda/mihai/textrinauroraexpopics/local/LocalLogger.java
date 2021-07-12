package com.amazonaws.lambda.mihai.textrinauroraexpopics.local;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
 * mock for AWS Cloud Logger object; logs to System.out
 * @author Mihai ADAM
 *
 */
public class LocalLogger implements LambdaLogger {

	@Override
	public void log(String string) {
		System.out.println(string);
	}

}
