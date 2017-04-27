/*
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
package com.facebook.presto.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class HiveWasbConfig
{
    private String wasbStorageAccount;
    private String wasbAccessKey;

    public String getWasbStorageAccount()
    {
        return wasbStorageAccount;
    }

    @ConfigSecuritySensitive
    @Config("hive.wasb.storage-account")
    public HiveWasbConfig setWasbStorageAccount(String wasbStorageAccount)
    {
        this.wasbStorageAccount = wasbStorageAccount;
        return this;
    }

    public String getWasbAccessKey()
    {
        return wasbAccessKey;
    }

    @ConfigSecuritySensitive
    @Config("hive.wasb.access-key")
    public HiveWasbConfig setWasbAccessKey(String wasbAccessKey)
    {
        this.wasbAccessKey = wasbAccessKey;
        return this;
    }
}
