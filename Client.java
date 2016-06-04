import java.io.*;
import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Client {

	public static void main(String[] args) throws FileNotFoundException,IOException {
		//Reading the user arguments
		
		String client=args[0];
		String s=args[1];
		String sqsname=args[2];
		String w=args[3];
		String workFile=args[4];
		
		//Amazon credentials
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (C:\\Users\\VENKATASIVANAGASAISU\\.aws\\credentials), and is in valid format.",
							e);
		}
		
		//Amazon SQS API
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
		System.out.println("===========================================");
		System.out.println("Pushing data to Amazon SQS");
		System.out.println("===========================================\n"); 
		try {
			// Create a queue
			System.out.println("Creating a new SQS queue called Queue.\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(sqsname);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			String line;
			int j=0;
			TreeMap<Integer,String> workload=new TreeMap<>();
			
			//Load a file
			File file=new File(workFile);
			FileReader fileReader=new FileReader(file);
			BufferedReader bufferedReader=new BufferedReader(fileReader);
			while((line=bufferedReader.readLine())!=null){
				j++;
				workload.put(j,line);
				//Sending message to SQS
				sqs.sendMessage(new SendMessageRequest(myQueueUrl, line));
			}
			AmazonDynamoDBSample dynamodb=new AmazonDynamoDBSample();
			dynamodb.db(myQueueUrl);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

}
