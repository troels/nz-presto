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
package com.facebook.presto.server.security;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KerberosConfig
{
    private File kerberosConfig;
    private String serviceName;
    private File keytab;
    private String realm;
    private List<String> principals;
    private List<String> keytabs;

    @NotNull
    public File getKerberosConfig()
    {
        return kerberosConfig;
    }

    @Config("http.authentication.krb5.config")
    public KerberosConfig setKerberosConfig(File kerberosConfig)
    {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    @NotNull
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("http.server.authentication.krb5.service-name")
    public KerberosConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    public File getKeytab()
    {
        return keytab;
    }

    @Config("http.server.authentication.krb5.keytab")
    public KerberosConfig setKeytab(File keytab)
    {
        this.keytab = keytab;
        return this;
    }


    public String getRealm() {
        return realm;
    }

    @Config("http.server.authentication.krb5.realm")
    public KerberosConfig setRealm(String realm) {
        this.realm = realm;
        return this;
    }

    @Config("http.server.authentication.principals")
    public KerberosConfig setPrincipals(String principals) {
        this.principals = Stream.of(principals.split(",")).map(String::trim).filter((x) -> !x.isEmpty()).collect(Collectors.toList());
        return this;
    }

    public List<String> getPrincipals() {
        return principals;
    }

    @Config("http.server.authentication.keytabs")
    public KerberosConfig setKeytabs(String keytabs) {
        this.keytabs = Stream.of(keytabs.split(",")).map(String::trim).filter((x) -> !x.isEmpty()).collect(Collectors.toList());
        return this;
    }

    public List<String> getKeytabs() {
        return keytabs;
    }
}
