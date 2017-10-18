package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ReplicatedRecordFilter}. Dated 10/18/17
 * @author <a href="https://github.com/anirban-roy">Anirban Roy</a>
 */
public class ReplicatedRecordFilterTests {

    private static final String FLAG_ATTRIBUTE = "replicated";
    private ReplicatedRecordFilter filter;

    @Before
    public void setup() {
        Properties properties = new Properties();
        properties.put(CommandLineArgs.REPLICATED_FLAG, FLAG_ATTRIBUTE);
        DynamoDBStreamsConnectorConfiguration config = new DynamoDBStreamsConnectorConfiguration(properties, null);
        filter = new ReplicatedRecordFilter(config);
    }

    @Test
    public void testRecordFilterPassed () {
        Map<String, AttributeValue> newImageNoReplication = new HashMap<>();
        newImageNoReplication.put("attr1", new AttributeValue().withS("testVal"));
        Record record = new Record();
        StreamRecord dynamodb = new StreamRecord();
        dynamodb.setNewImage(newImageNoReplication);
        record.setDynamodb(dynamodb);
        assertTrue(filter.keepRecord(record));
    }

    @Test
    public void testRecordFilterFiltered () {
        Map<String, AttributeValue> newImageNoReplication = new HashMap<>();
        newImageNoReplication.put("attr1", new AttributeValue().withS("testVal"));
        newImageNoReplication.put(FLAG_ATTRIBUTE, new AttributeValue().withBOOL(true));
        Record record = new Record();
        StreamRecord dynamodb = new StreamRecord();
        dynamodb.setNewImage(newImageNoReplication);
        record.setDynamodb(dynamodb);
        assertFalse(filter.keepRecord(record));
    }
}
