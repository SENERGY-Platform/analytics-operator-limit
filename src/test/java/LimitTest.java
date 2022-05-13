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

import org.infai.ses.senergy.models.DeviceMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.testing.utils.JSONHelper;
import org.infai.ses.senergy.utils.ConfigProvider;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.junit.Assert;
import org.junit.Test;

public class LimitTest {


    public void run(Config config) throws InterruptedException {
        JSONArray messages = new JSONHelper().parseFile("sample-data-small.json");

        String topicName = config.getInputTopicsConfigs().get(0).getName();
        ConfigProvider.setConfig(config);
        Message message = new Message();
        BaseOperator op = new Limit(1000);

        int counter = 0;
        for (Object m : messages) {
            MessageModel model = new MessageModel();
            op.configMessage(message);

            DeviceMessageModel deviceMessageModel = JSONHelper.getObjectFromJSONString(m.toString(), DeviceMessageModel.class);
            assert deviceMessageModel != null;
            model.putMessage(topicName, Helper.deviceToInputMessageModel(deviceMessageModel, topicName));
            message.setMessage(model);
            op.run(message);
            switch (counter) {
                case 0:
                case 3:
                    Assert.assertFalse(message.getMessage().getOutputMessage().getAnalytics().isEmpty());
                    Thread.sleep(100);
                    break;
                case 1:
                    Assert.assertTrue(message.getMessage().getOutputMessage().getAnalytics().isEmpty());
                    Thread.sleep(1001);
                    break;
            }

            counter++;
        }
    }

    @Test
    public void test() throws Exception {
        JSONObject jsonConfig = new JSONObject(getConfig());
        jsonConfig.put("config", new JSONObject("{\"title\":\"someTitle %s\",\"message\":\"just a msg\"}"));
        run(new Config(jsonConfig.toString()));
    }


    private static String getConfig() {
        return "{\n" +
                "  \"inputTopics\": [\n" +
                "    {\n" +
                "      \"name\": \"iot_bc59400c-405c-4c84-9862-a791daa82b60\",\n" +
                "      \"filterType\": \"DeviceId\",\n" +
                "      \"filterValue\": \"0\",\n" +
                "      \"mappings\": [\n" +
                "        {\n" +
                "          \"dest\": \"value\",\n" +
                "          \"source\": \"value.reading.value\"\n" +
                "        }" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }
}
