package org.locationtech.geomesa.dynamodb.data;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.collect.ImmutableMap;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class DynamoDBJavaDataStoreIT {

    @Test
    public void testDataAccess() throws IOException {
        DataStore db = getDataStore();
        System.out.println("BLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        assert db != null;
    }

    public DataStore getDataStore() throws IOException {
        Map<String, ?> params = ImmutableMap.of(
                DynamoDBDataStoreFactory.CATALOG().getName(), "ddbtest",
                DynamoDBDataStoreFactory.DYNAMODBAPI().getName(), getNewDynamoDB()
        );
        return DataStoreFinder.getDataStore(params);
    }

    public DynamoDB getNewDynamoDB() {
        AmazonDynamoDBClient adbc = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
        adbc.setEndpoint(String.format("http://localhost:%s", System.getProperty("dynamodb.port")));
        return new DynamoDB(adbc);
    }

}
