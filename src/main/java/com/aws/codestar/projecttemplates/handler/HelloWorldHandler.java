package com.aws.codestar.projecttemplates.handler;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.aws.codestar.projecttemplates.GatewayResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Handler for requests to Lambda function.
 */
public class HelloWorldHandler implements RequestHandler<Object, Object> {

	 Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
    public Object handleRequest(final Object input, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + gson.toJson(context));
        // process event
        logger.log("EVENT: " + gson.toJson(input));
        
        
        return new GatewayResponse(new JSONObject().put("Output", "Hello World!, Olivier Maillet").toString(), headers, 200);
    }
}
