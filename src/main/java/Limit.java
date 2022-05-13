/*
 * Copyright 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.Message;

import java.io.IOException;

public class Limit extends BaseOperator {
    private final long limitMs;

    private long lastMsg = 0;
    private final boolean DEBUG;

    public Limit(long limitMs) {
        this.limitMs = limitMs;
        DEBUG = Boolean.parseBoolean(Helper.getEnv("DEBUG", "false"));
    }

    @Override
    public void run(Message m) {
        try {
            Object in = m.getFlexInput("value").getValue(Object.class);
            long now = System.currentTimeMillis();
            if (lastMsg + limitMs <= now) {
                if (DEBUG) {
                    System.out.println("Allowing message to pass");
                }
                lastMsg = now;
                m.output("value", in);
            } else {
                if (DEBUG) {
                    System.out.println("Blocking message, still waiting for " + ((lastMsg + limitMs) - now) + "ms");
                }
            }
        } catch (NoValueException e) {
            System.err.println("Error getting a value, skipping message....");
            e.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addFlexInput("value");
        return message;
    }
}
