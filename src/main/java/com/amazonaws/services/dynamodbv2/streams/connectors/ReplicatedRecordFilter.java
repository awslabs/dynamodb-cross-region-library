package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import lombok.extern.log4j.Log4j;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * An implementation of {@link IFilter} to keep out records which has the replicated flag attribute set to be <i>true</i>.
 * Passes/keeps everything else.
 * Dated 10/18/17
 * @author <a href="https://github.com/anirban-roy">Anirban Roy</a>
 */
@Log4j
public class ReplicatedRecordFilter implements IFilter<Record> {
    private final String replicatedAttibute; // should not be modified mid runtime

    public ReplicatedRecordFilter(DynamoDBStreamsConnectorConfiguration configuration) {
        this.replicatedAttibute = configuration.REPLICATED_FLAG;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keepRecord(Record record) {
        if (isNotBlank(replicatedAttibute)){
            Map<String, AttributeValue> image = null;
            if (record.getDynamodb() != null) {
                image = record.getDynamodb().getNewImage();
            }
            if (image != null) {
                AttributeValue flagValue = image.get(replicatedAttibute);
                if (flagValue == null) {
                    image.put(replicatedAttibute, new AttributeValue().withBOOL(true));
                    return true;
                } else {
                    Boolean flag = flagValue.getBOOL();
                    if (flag !=null && flag) {
                        log.debug("Filtering out record, Event Id: " + record.getEventID());
                        return false;
                    } else {
                        image.put(replicatedAttibute, new AttributeValue().withBOOL(true));
                        return true;
                    }
                }
            }
        }
        return true;
    }
}
