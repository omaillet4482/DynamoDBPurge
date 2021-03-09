package com.aws.codestar.projecttemplates.handler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.aws.codestar.projecttemplates.GatewayResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Handler for requests to Lambda function.
 */
public class DynamoDomoticzHandler implements RequestHandler<Object, Object> {

	 Gson gson = new GsonBuilder().setPrettyPrinting().create();
	 
	 private DynamoDB dynamoDb;
	    private String DYNAMODB_TABLE_NAME = "DomoticEvent";
	    private Regions REGION = Regions.EU_WEST_3;
	
	    private AmazonDynamoDB clientDb; 
	    
    public Object handleRequest(final Object input, final Context context) {
    	
    	initDynamoDbClient();
    	
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + gson.toJson(context));
        // process event
        logger.log("EVENT: " + gson.toJson(input));
        
        Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);

        Map<String, Object> expressionAttributeValues =
        	    new HashMap<String, Object>();
        	expressionAttributeValues.put(":date", 100);
        
        ScanSpec spec = new ScanSpec();
        
        spec.withFilterExpression("horodatage > :date")
        .withProjectionExpression("idx, horodatage, nom, valeur1, expire")
        .withValueMap(expressionAttributeValues)
        .withMaxResultSize(10);
        
   

       

        System.out.println("Scan of " + DYNAMODB_TABLE_NAME + " for items with a price less than 100.");
        ItemCollection<ScanOutcome> items = table.scan(spec);
        Iterator<Item> iterator = items.iterator();
        JSONObject result = new JSONObject();
        while (iterator.hasNext()) {
            result.append("results", iterator.next().toJSONPretty());
        }
    
        
        
        return new GatewayResponse(result.toString(), headers, 200);
    }
    
    
    private void initDynamoDbClient() {
    	
    	
    	AmazonDynamoDBClientBuilder clientDbtmp = AmazonDynamoDBClientBuilder.standard();
    	clientDbtmp.setRegion(REGION.getName());
    	
        this.clientDb = clientDbtmp.build();
        this.dynamoDb = new DynamoDB(clientDb);
    }
}
