/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.redshift.bridge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2; // OK
import org.apache.atlas.model.instance.AtlasEntity; // ok
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo; // ok
import org.apache.atlas.model.instance.EntityMutationResponse; // ok
import org.apache.atlas.utils.AuthenticationUtil; // ok
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException; // ok
import org.apache.commons.configuration.Configuration; // ok
import org.apache.hadoop.security.UserGroupInformation; // ok
import org.slf4j.Logger; // ok
import org.slf4j.LoggerFactory; // ok

public class RedshiftBridge {
    private static final Logger LOG = LoggerFactory.getLogger(RedshiftBridge.class);

    static final String redshiftDriver = "com.amazon.redshift.jdbc42.Driver";
    static final String redshiftUrl = "jdbc:redshift://production-dwh.cuqhe6ylr9kz.eu-west-1.redshift.amazonaws.com:5439/analytics";
    static final String masterUsername = "yinkit";
    static final String password = "ABPnnwaYMVEmOGean4Pj";

    public static final String CONF_PREFIX = "atlas.hook.hive.";
    public static final String CLUSTER_NAME_KEY = "atlas.cluster.name";
    public static final String REDSHIFT_METADATA_NAMESPACE = "atlas.metadata.namespace";
    public static final String DEFAULT_CLUSTER_NAME = "primary";
    public static final String TEMP_TABLE_PREFIX = "_temp-";
    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED = 1;
    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";

    private final String metadataNamespace;
    private final AtlasClientV2 atlasClientV2;

    /*public static void main(String[] args) {
        System.out.println("Entry");
        Connection connection = null;
        Statement statement = null;

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(redshiftDriver);
        dataSource.setUrl(redshiftUrl);
        dataSource.setUsername(masterUsername);
        dataSource.setPassword(password);

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT country_id FROM backend.cities WHERE name = 'Lyon'";

        //String name =  jdbcTemplate.queryForObject(sql, String.class);
        // Further code to follow



        System.out.println("Exit");
    }*/

    public static void main(String[] args) {
        int exitCode = EXIT_CODE_FAILED;
        AtlasClientV2 atlasClientV2 = null;

        try {
            Options options = new Options();
            options.addOption("d", "database", true, "Database name");
            options.addOption("t", "table", true, "Table name");
            options.addOption("f", "filename", true, "Filename");
            options.addOption("failOnError", false, "failOnError");

            CommandLine cmd              = new BasicParser().parse(options, args);
            boolean       failOnError      = cmd.hasOption("failOnError");
            String        databaseToImport = cmd.getOptionValue("d");
            String        tableToImport    = cmd.getOptionValue("t");
            String        fileToImport     = cmd.getOptionValue("f");
            Configuration atlasConf        = ApplicationProperties.get();
            String[]      atlasEndpoint    = atlasConf.getStringArray(ATLAS_ENDPOINT);

            if (atlasEndpoint == null || atlasEndpoint.length == 0) {
                atlasEndpoint = new String[] { DEFAULT_ATLAS_URL };
            }


            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = {"admin", "admin"};
                System.out.println("basic auth: " + basicAuthUsernamePassword.toString());
                atlasClientV2 = new AtlasClientV2(atlasEndpoint, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                System.out.println("ugi auth : " + ugi.getShortUserName());
                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), atlasEndpoint);
            }

            RedshiftBridge redshiftBridge = new RedshiftBridge(atlasConf, atlasClientV2);

            // search tables

            AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo(toDbEntity(null));
            EntityMutationResponse response        = atlasClientV2.createEntity(entity);

            System.out.println(response.toString());

            exitCode = EXIT_CODE_SUCCESS;
        } catch(ParseException e) {
            LOG.error("Failed to parse arguments. Error: ", e.getMessage());
            printUsage();
        } catch(Exception e) {
            LOG.error("Import failed", e);
        } finally {
            if( atlasClientV2 !=null) {
                atlasClientV2.close();
            }
        }
        System.out.println("Success !");
        System.exit(exitCode);
    }


    private AtlasEntity toDbEntity() {
        return toDbEntity(null);
    }

    private static AtlasEntity toDbEntity(AtlasEntity dbEntity) {
        if (dbEntity == null) {
            dbEntity = new AtlasEntity("Redshift DB");
        }

        dbEntity.setAttribute("qualifiedName", "namespace DB");
        dbEntity.setAttribute("name", "backend");
        dbEntity.setAttribute("description", "this is a description");
        dbEntity.setAttribute("owner", "this is the owner");

        return dbEntity;
    }

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
