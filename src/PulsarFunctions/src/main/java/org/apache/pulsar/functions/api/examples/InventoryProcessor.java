/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.api.examples;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import java.nio.ByteBuffer;


/**
 * The classic word count example done using pulsar functions Each input message
 * is a sentence that split into words and each word counted. The built in
 * counter state is used to keep track of the word count in a persistent and
 * consistent manner.
 */
public class InventoryProcessor implements Function<String, String> {

    public void respond(String msg, Context context){
        Record<?> rec = context.getCurrentRecord();
        String topicName = rec.getTopicName().get();
        String key = rec.getKey().get();
        try {
            context.newOutputMessage(topicName + "output",
                Schema.STRING)
                    .value(msg)
                    .key(key)
                    .send();
        } catch (PulsarClientException e) {
            context.getLogger().error("Response Error", e);
        }
    }

    @Override
    public String process(String input, Context context) throws PulsarClientException {
        Logger logger = context.getLogger();
        long sequenceNumber = context.getCurrentRecord().getRecordSequence().get();
        String[] a = input.split("\\|");
        String method = a[0];
        String productId = a[1];
        int oldCount = 0;
        long oldSequenceNumber = 0;
        try {
            ByteBuffer buffer = context.getState(productId);
            buffer.rewind();
            oldCount = buffer.getInt();
            oldSequenceNumber = buffer.getLong();
        } catch (Exception e) {
            logger.info(e.toString());
            logger.info("Counter doesn't exist");
        }
        if (oldSequenceNumber > sequenceNumber){
            logger.info("Skip old command");
            return null;
        }
        switch (method) {
            case "GET":
                respond("" + oldCount, context);
                return null;
            case "ADD":
                if (oldSequenceNumber == sequenceNumber){
                    logger.info("Resending event");
                    respond("" + oldCount, context);
                    return input;
                }
                int operationCount = Integer.parseInt(a[2]);
                int newCount = oldCount + operationCount;
                if (newCount >= 0) {
                    ByteBuffer buffer = ByteBuffer.allocate(12);
                    buffer.putInt(newCount);
                    buffer.putLong(sequenceNumber);
                    buffer.rewind();
                    context.putState(productId, buffer);
                    respond("" + newCount, context);
                    return input;
                } else {
                    logger.error("Couldn't increment product: " + productId + " by " + operationCount);
                    respond("Validation error", context);
                    return null;
                }
            default:
                return null;
        }
    }
}
