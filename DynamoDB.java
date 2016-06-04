import java.text.SimpleDateFormat;
import java.util.ArrayList;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class CreateTablesLoadData {

    static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
            new ProfileCredentialsProvider()));

    static SimpleDateFormat dateFormatter = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static String productCatalogTableName = "ProductCatalog";
    static String forumTableName = "Forum";
    static String threadTableName = "Thread";
    static String replyTableName = "Reply";

    public static void main(String[] args) throws Exception {

        try {

            createTable(productCatalogTableName, 10L, 5L, "Id", "N");

        } catch (Exception e) {
            System.err.println("Program failed:");
            System.err.println(e.getMessage());
        }
        System.out.println("Success.");
    }

    private static void createTable(
        String tableName, long readCapacityUnits, long writeCapacityUnits, 
        String partitionKeyName, String partitionKeyType) {

        createTable(tableName, readCapacityUnits, writeCapacityUnits,
            partitionKeyName, partitionKeyType, null, null);
    }

    private static void createTable(
        String tableName, long readCapacityUnits, long writeCapacityUnits, 
        String partitionKeyName, String partitionKeyType, 
        String sortKeyName, String sortKeyType) {

        try {

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                .withAttributeName(partitionKeyName)
                .withKeyType(KeyType.HASH)); //Partition key
            
            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(partitionKeyName)
                .withAttributeType(partitionKeyType));

            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement()
                    .withAttributeName(sortKeyName)
                    .withKeyType(KeyType.RANGE)); //Sort key
                attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(sortKeyName)
                    .withAttributeType(sortKeyType));
            }

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withProvisionedThroughput( new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));

            // If this is the Reply table, define a local secondary index
            if (replyTableName.equals(tableName)) {
                
                attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName("PostedBy")
                    .withAttributeType("S"));

                ArrayList<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
                localSecondaryIndexes.add(new LocalSecondaryIndex()
                    .withIndexName("PostedBy-Index")
                    .withKeySchema(
                        new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH),  //Partition key
                        new KeySchemaElement() .withAttributeName("PostedBy") .withKeyType(KeyType.RANGE))  //Sort key
                    .withProjection(new Projection() .withProjectionType(ProjectionType.KEYS_ONLY)));

                request.setLocalSecondaryIndexes(localSecondaryIndexes);
            }

            request.setAttributeDefinitions(attributeDefinitions);

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = dynamoDB.createTable(request);
            System.out.println("Waiting for " + tableName
                + " to be created...this may take a while...");
            table.waitForActive();

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    public static void runSleepJob(int id,int task) throws InterruptedException {
    	Table table = dynamoDB.getTable(productCatalogTableName);
    	Item item=new Item().withPrimaryKey("Id", id);
    	String tableId;
    	table.putItem(item);
    	//System.out.println();
    	Thread.sleep(id);

    	//Dynamo DB check
    	if(table.getItem("Id",id).toString().split(" |\\=|\\}")[3]!=Integer.toString(id)){
    		table.putItem(item);
    		
    		System.out.println("Thread slept for "+ id);
    		
    	}
    	else {
			System.out.println("Datais already There");
		}
	}
    
    public static int split(String tableId){
    	tableId=tableId.split(" ")[2];
    	String splitByEqual=tableId.split("=")[1];
    	String splitByBrace=splitByEqual.split("}")[0];
    	System.out.println(splitByBrace);
		return 0;
    	
    }
    
}
