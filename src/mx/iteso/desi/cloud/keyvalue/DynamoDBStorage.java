package mx.iteso.desi.cloud.keyvalue;

import java.util.Set;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import mx.iteso.desi.cloud.hw1.Config;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class DynamoDBStorage extends BasicKeyValueStore {

    static String tableName = "ProductCatalog";
    String endPoint = "dynamodb.us-east-1.amazonaws.com";

    BasicAWSCredentials credentials = new BasicAWSCredentials(Config.accessKeyID, Config.secretAccessKey);
    AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials).withRegion(Regions.US_EAST_1).withEndpoint(endPoint);
    //client.setEndpoint("dynamodb.us-east-1.amazonaws.com");
    DynamoDB dynamoDB = new DynamoDB(client);

    String dbName;

    // Simple autoincrement counter to make sure we have unique entries
    int inx = 0;
    int inxT = 0;

    Set<String> attributesToGet = new HashSet<String>();

    //DynamoDB dynamoDB;
    //static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
    //        new ProfileCredentialsProvider()));
    public DynamoDBStorage(String dbName) {
        this.dbName = dbName;

    }
 private  void retrieveItem() {
        Table table = dynamoDB.getTable(tableName);

        try {

            Item item = table.getItem("Id", 120,  "Id, ISBN, Title, Authors", null);

            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }   

    }
    @Override
    public Set<String> get(String search) {

        Set<String> result = new HashSet<String>();

        try {

            String tableName = dbName;
            Table table = dynamoDB.getTable(tableName);

            //1QuerySpec spec = new QuerySpec()
                    //.withKeyConditionExpression("Id = keyword")
                    //.withValueMap(new ValueMap().withString("keyword", search));
               //1     .withHashKey("keyword", search);
            QuerySpec spec = new QuerySpec()
                   //.withKeyConditionExpression("keyword = :v_keyid and IsOpen = :v_isopen")
                  .withKeyConditionExpression("keyword = :v_keyid ")
                  .withValueMap(new ValueMap()
                    .withString(":v_keyid", search)
                  //  .withNumber(":v_isopen", 1)
                  );

            ItemCollection<QueryOutcome> items = table.query(spec);

            Iterator<Item> iterator = items.iterator();
            Item item = null;
            while (iterator.hasNext()) {
                item = iterator.next();
                result.add(item.get("value").toString());
                System.out.println(item.toJSONPretty());
            }

            /*
            Item item = table.getItem("inx", inx, "Id, ISBN, Title, Authors", null);

            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());
             */
        } catch (Exception e) {
            System.err.println(e.toString());
            System.err.println(e.getMessage());
        }

        return result;
    }

    @Override
    public boolean exists(String search) {
        return get(search).size() > 0;
    }

    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addToSet(String keyword, String value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean existTable(String tableName) {
        try {
            System.out.println("Describing " + tableName);

            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            /*System.out.format("Name: %s:\n" + "Status: %s \n"
                    + "Provisioned Throughput (read capacity units/sec): %d \n"
                    + "Provisioned Throughput (write capacity units/sec): %d \n",
                    tableDescription.getTableName(),
                    tableDescription.getTableStatus(),
                    tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                    tableDescription.getProvisionedThroughput().getWriteCapacityUnits());*/
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public void put(String keyword, String value) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.       

        if (inx == 0 && this.dbName.equals("images") && !existTable("images") ) {
            //deleteTable("images");
            createTable("images");
        }  
        if (this.dbName.equals("images")) {
            createItems("images", inx++, keyword, value);
        }
        if (inxT == 0  && this.dbName.equals("terms") && !existTable("terms")) {
            //deleteTable("terms");
            createTable("terms");
        }
        if (this.dbName.equals("terms")) {
            createItems("terms", inxT++, keyword, value);
        }

        System.out.println("Putting keyword: " + keyword + " value: " + value);

    }

    @Override
    public void close() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean isCompressible() {
        return false;
    }

    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }

    private void createTable(String tableName) {

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("keyword", KeyType.HASH), //Partition key
                            new KeySchemaElement("inx", KeyType.RANGE) //Sort key                            
                    ),
                    Arrays.asList(
                            new AttributeDefinition("keyword", ScalarAttributeType.S),
                            new AttributeDefinition("inx", ScalarAttributeType.N)
                    ),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }

    private void createItems(String tableName, int inx, String keyword, String value) {

        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = new Item()
                    .withPrimaryKey("inx", inx)
                    .withString("keyword", keyword)
                    .withString("value", value) //.withStringSet("Value",
                    //        new HashSet<String>(Arrays.asList("Author12", "Author22")))
                    ;
            table.putItem(item);
            /*
            item = new Item()
                    .withPrimaryKey("Id", 121)
                    .withString("Title", "Book 121 Title")
                    .withString("ISBN", "121-1111111111")
                    .withStringSet("Authors",
                            new HashSet<String>(Arrays.asList("Author21", "Author 22")))
                    .withNumber("Price", 20)
                    .withString("Dimensions", "8.5x11.0x.75")
                    .withNumber("PageCount", 500)
                    .withBoolean("InPublication", true)
                    .withString("ProductCategory", "Book");
            table.putItem(item);
             */
        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());

        }
    }

    private void retrieveMaxItem(String tableName) {
        Table table = dynamoDB.getTable(tableName);

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("Id = inx") //.withValueMap(new ValueMap().withString(":v_id", "Amazon DynamoDB#DynamoDB Thread 1"))
                ;

        ItemCollection<QueryOutcome> items = table.query(spec);

        Iterator<Item> iterator = items.iterator();
        Item item = null;
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.toJSONPretty());
        }
    }

    private void retrieveItem(String tableName, int inx) {
        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = table.getItem("inx", inx, "Id, ISBN, Title, Authors", null);

            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }
    }

    private void updateAddNewAttribute() {
        Table table = dynamoDB.getTable(tableName);
        try {
            Map<String, String> expressionAttributeNames = new HashMap<String, String>();
            expressionAttributeNames.put("#na", "NewAttribute");

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 121)
                    .withUpdateExpression("set #na = :val1")
                    .withNameMap(new NameMap()
                            .with("#na", "NewAttribute"))
                    .withValueMap(new ValueMap()
                            .withString(":val1", "Some value"))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after adding new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Failed to add new attribute in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private void updateMultipleAttributes() {

        Table table = dynamoDB.getTable(tableName);

        try {

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withUpdateExpression("add #a :val1 set #na=:val2")
                    .withNameMap(new NameMap()
                            .with("#a", "Authors")
                            .with("#na", "NewAttribute"))
                    .withValueMap(new ValueMap()
                            .withStringSet(":val1", "Author YY", "Author ZZ")
                            .withString(":val2", "someValue"))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out
                    .println("Printing item after multiple attribute update...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Failed to update multiple attributes in "
                    + tableName);
            System.err.println(e.getMessage());

        }
    }

    private void updateExistingAttributeConditionally() {

        Table table = dynamoDB.getTable(tableName);

        try {

            // Specify the desired price (25.00) and also the condition (price =
            // 20.00)
            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withReturnValues(ReturnValue.ALL_NEW)
                    .withUpdateExpression("set #p = :val1")
                    .withConditionExpression("#p = :val2")
                    .withNameMap(new NameMap()
                            .with("#p", "Price"))
                    .withValueMap(new ValueMap()
                            .withNumber(":val1", 25)
                            .withNumber(":val2", 20));

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out
                    .println("Printing item after conditional update to new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error updating item in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private void deleteItem() {

        Table table = dynamoDB.getTable(tableName);

        try {

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withConditionExpression("#ip = :val")
                    .withNameMap(new NameMap()
                            .with("#ip", "InPublication"))
                    .withValueMap(new ValueMap()
                            .withBoolean(":val", false))
                    .withReturnValues(ReturnValue.ALL_OLD);

            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error deleting item in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private void deleteTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            table.delete();
            System.out.println("Waiting for " + tableName
                    + " to be deleted...this may take a while...");
            table.waitForDelete();

        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }
}
