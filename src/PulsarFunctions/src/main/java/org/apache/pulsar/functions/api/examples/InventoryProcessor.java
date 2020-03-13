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

import java.util.Arrays;
import java.util.Optional;

/**
 * The classic word count example done using pulsar functions Each input message
 * is a sentence that split into words and each word counted. The built in
 * counter state is used to keep track of the word count in a persistent and
 * consistent manner.
 */
public class InventoryProcessor implements Function<byte[], Void> {

    public void respond(String msg, Context context){
        Record<?> rec = context.getCurrentRecord();
        String topicName = rec.getTopicName().get();
        String key = rec.getKey().get();
        try {
            context.newOutputMessage(topicName + "output", Schema.STRING).value(msg).key(key).send();
        } catch (PulsarClientException e) {
            context.getLogger().error("Response Error", e);
        }
    }

    @Override
    public Void process(byte[] bytes, Context context) {
        String input = new String(bytes);
        Logger logger = context.getLogger();
        String[] a = input.split("\\|");
        String method = a[0];
        String productId = a[1];;
        long currentCount = 0;
        try {
            currentCount = context.getCounter(productId);
        } catch (Exception e) {
            logger.info("Conter doesn't exist");
        }

        switch (method) {
            case "GET":
                respond(Long.toString(currentCount), context);
                break;
            case "ADD":
                long count = Long.parseLong(a[2]);
                long newCount = currentCount + count;
                if (newCount >= 0) {
                    context.incrCounter(productId, count);
                    logger.info("Successfully incremented" + productId + " by" + count);
                    try {
                        context.newOutputMessage("inventoryAdjusted", Schema.STRING).value(input).send();
                        respond("" + newCount, context);
                    } catch (PulsarClientException e) {
                        context.incrCounter(productId, -count);
                        logger.error("Send Error", e);
                        respond("Unexpected error", context);
                    }
                } else {
                    logger.error("Couldn't increment" + productId + " by" + count);
                    respond("Validation error", context);
                }
                break;
            default:
                break;
        }


        return null;
    }
}
