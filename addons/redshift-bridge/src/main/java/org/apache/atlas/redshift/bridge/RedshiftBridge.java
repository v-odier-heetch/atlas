/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.redshift.bridge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.sql.Statement;

public class RedshiftBridge {
    private static final Logger LOG = LoggerFactory.getLogger(RedshiftBridge.class);

    static final String redshiftDriver = "com.amazon.redshift.jdbc42.Driver";
    static final String redshiftUrl = "jdbc:redshift://production-dwh.cuqhe6ylr9kz.eu-west-1.redshift.amazonaws.com:5439/analytics";
    static final String masterUsername = "yinkit";
    static final String password = "ABPnnwaYMVEmOGean4Pj";

    private static final String CLUSTER_NAME_KEY = "atlas.cluster.name";
    private static final String DEFAULT_CLUSTER_NAME = "primary";
    private static final String REDSHIFT_METADATA_NAMESPACE = "atlas.metadata.namespace";

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED = 1;

    private final String metadataNamespace;
    private final AtlasClientV2 atlasClientV2;

    public static void main(String[] args) {
        System.out.println("coucou");
        Connection connection = null;
        Statement statement = null;

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        /*dataSource.setDriverClassName(redshiftDriver);
        dataSource.setUrl(redshiftUrl);
        dataSource.setUsername(masterUsername);
        dataSource.setPassword(password);*/

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT country_id FROM backend.cities WHERE name = 'Lyon'";

        //String name =  jdbcTemplate.queryForObject(sql, String.class);
        // Further code to follow
        System.out.println("Coucou");
    }

    /**public static void main(String[] args) {
     int exitCode = EXIT_CODE_FAILED;
     AtlasClientV2 atlasClientV2 = null;
     try {
     Configuration atlasConf = ApplicationProperties.get();
     } catch (Exception e) {
     LOG.error("Import failed", e);
     } finally {
     if (atlasClientV2 != null) {
     atlasClientV2.close();
     }
     }

     System.exit(exitCode);
     }*/

    public RedshiftBridge(Configuration atlasConf, AtlasClientV2 atlasClientV2) {
        this.metadataNamespace = getMetadataNamespace(atlasConf);
        this.atlasClientV2 = atlasClientV2;
    }

    private String getMetadataNamespace(Configuration config) {
        return config.getString(REDSHIFT_METADATA_NAMESPACE, getClusterName(config));
    }

    private String getClusterName(Configuration config) {
        return config.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    private static void printUsage() {
        System.out.println();
        System.out.println();
        System.out.println("Usage 1: import-hive.sh [-d <database> OR --database <database>] ");
        System.out.println("    Imports specified database and its tables ...");
        System.out.println();
        System.out.println("Usage 2: import-hive.sh [-d <database> OR --database <database>] [-t <table> OR --table <table>]");
        System.out.println("    Imports specified table within that database ...");
        System.out.println();
        System.out.println("Usage 3: import-hive.sh");
        System.out.println("    Imports all databases and tables...");
        System.out.println();
        System.out.println("Usage 4: import-hive.sh -f <filename>");
        System.out.println("  Imports all databases and tables in the file...");
        System.out.println("    Format:");
        System.out.println("    database1:tbl1");
        System.out.println("    database1:tbl2");
        System.out.println("    database2:tbl2");
        System.out.println();
    }
}
