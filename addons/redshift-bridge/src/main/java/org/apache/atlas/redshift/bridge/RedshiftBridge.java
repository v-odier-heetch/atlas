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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.redshift.model.SvvColumn;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME;
import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectId;
import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectIds;

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

    public static final String ATLAS_REST_ADDRESS          = "atlas.rest.address";

    public static final String SALES_DB                    = "Sales";
    public static final String REPORTING_DB                = "Reporting";
    public static final String LOGGING_DB                  = "Logging";

    public static final String SALES_FACT_TABLE            = "sales_fact";
    public static final String PRODUCT_DIM_TABLE           = "product_dim";
    public static final String CUSTOMER_DIM_TABLE          = "customer_dim";
    public static final String TIME_DIM_TABLE              = "time_dim";
    public static final String SALES_FACT_DAILY_MV_TABLE   = "sales_fact_daily_mv";
    public static final String SALES_FACT_MONTHLY_MV_TABLE = "sales_fact_monthly_mv";
    public static final String LOG_FACT_DAILY_MV_TABLE     = "log_fact_daily_mv";
    public static final String LOG_FACT_MONTHLY_MV_TABLE   = "logging_fact_monthly_mv";

    public static final String TIME_ID_COLUMN              = "time_id";
    public static final String PRODUCT_ID_COLUMN           = "product_id";
    public static final String CUSTOMER_ID_COLUMN          = "customer_id";
    public static final String APP_ID_COLUMN               = "app_id";
    public static final String MACHINE_ID_COLUMN           = "machine_id";
    public static final String PRODUCT_NAME_COLUMN         = "product_name";
    public static final String BRAND_NAME_COLUMN           = "brand_name";
    public static final String NAME_COLUMN                 = "name";
    public static final String SALES_COLUMN                = "sales";
    public static final String LOG_COLUMN                  = "log";
    public static final String ADDRESS_COLUMN              = "address";
    public static final String DAY_OF_YEAR_COLUMN          = "dayOfYear";
    public static final String WEEKDAY_COLUMN              = "weekDay";

    public static final String DIMENSION_CLASSIFICATION    = "Dimension";
    public static final String FACT_CLASSIFICATION         = "Fact";
    public static final String PII_CLASSIFICATION          = "PII";
    public static final String METRIC_CLASSIFICATION       = "Metric";
    public static final String ETL_CLASSIFICATION          = "ETL";
    public static final String JDBC_CLASSIFICATION         = "JdbcAccess";
    public static final String LOGDATA_CLASSIFICATION      = "Log Data";

    public static final String DATABASE_TYPE               = "DB";
    public static final String COLUMN_TYPE                 = "Column";
    public static final String TABLE_TYPE                  = "Table";
    public static final String STORAGE_DESC_TYPE           = "StorageDesc";

    public static final String MANAGED_TABLE               = "Managed";
    public static final String EXTERNAL_TABLE              = "External";
    public static final String CLUSTER_SUFFIX              = "@cl1";

    public static final String[] TYPES = { DATABASE_TYPE, TABLE_TYPE, STORAGE_DESC_TYPE, COLUMN_TYPE,
            JDBC_CLASSIFICATION, ETL_CLASSIFICATION, METRIC_CLASSIFICATION, PII_CLASSIFICATION, FACT_CLASSIFICATION,
            DIMENSION_CLASSIFICATION, LOGDATA_CLASSIFICATION};

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
            // Search tables
            System.out.println("Searching entities ...");
            List<SvvColumn> columns = redshiftBridge.getAll(redshiftBridge.getJdbcTemplate());

            // load from CSV
            //List<SvvColumn> columns = redshiftBridge.fromCsv();
            // Create Entities
            System.out.println("Registering Entities ...");
            redshiftBridge.registerEntities(columns);


            exitCode = EXIT_CODE_SUCCESS;
        } catch(ParseException e) {
            System.out.println("Failed to parse arguments. Error: " + e.getMessage());
            printUsage();
        } catch(Exception e) {
            System.out.println("Import failed" +  e.toString());
        } finally {
            if( atlasClientV2 !=null) {
                atlasClientV2.close();
            }
        }
        System.exit(exitCode);
    }

    private List<SvvColumn> fromCsv() throws IOException {
        File csvFile = new File("/home/chongyk/works/heetch/atlas-fork/atlas/addons/redshift-bridge/src/main/resources/redshift_with_header.csv");
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        MappingIterator<SvvColumn> personIter = new CsvMapper()
                .readerWithTypedSchemaFor(SvvColumn.class)
                .with(schema)
                .readValues(csvFile);

        return personIter.readAll();
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

    private JdbcTemplate getJdbcTemplate() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(redshiftDriver);
        dataSource.setUrl(redshiftUrl);
        dataSource.setUsername(masterUsername);
        dataSource.setPassword(password);

        return new JdbcTemplate(dataSource);
    }

    private List<SvvColumn> getAll(JdbcTemplate jdbcTemplate) {
        String sql = "SELECT SVV_TABLES.table_type, \n" +
                "        SVV_TABLES.remarks as table_remarks,\n" +
                "        SVV_TABLES.table_catalog as catalog,\n" +
                "        SVV_TABLES.table_schema as schema,\n" +
                "        SVV_COLUMNS.table_name,\n" +
                "        SVV_COLUMNS.column_name,\n" +
                "        SVV_COLUMNS.ordinal_position,\n" +
                "        SVV_COLUMNS.column_default,\n" +
                "        SVV_COLUMNS.is_nullable,\n" +
                "        SVV_COLUMNS.data_type,\n" +
                "        SVV_COLUMNS.character_maximum_length,\n" +
                "        SVV_COLUMNS.numeric_precision,\n" +
                "        SVV_COLUMNS.numeric_precision_radix,\n" +
                "        SVV_COLUMNS.numeric_scale,\n" +
                "        SVV_COLUMNS.datetime_precision,\n" +
                "        SVV_COLUMNS.interval_type,\n" +
                "        SVV_COLUMNS.interval_precision,\n" +
                "        SVV_COLUMNS.character_set_catalog,\n" +
                "        SVV_COLUMNS.character_set_schema,\n" +
                "        SVV_COLUMNS.character_set_name,\n" +
                "        SVV_COLUMNS.collation_catalog,\n" +
                "        SVV_COLUMNS.collation_schema,\n" +
                "        SVV_COLUMNS.collation_name,\n" +
                "        SVV_COLUMNS.domain_name,\n" +
                "        SVV_COLUMNS.remarks as column_remarks\n" +
                "FROM SVV_COLUMNS\n" +
                "JOIN SVV_TABLES ON SVV_TABLES.table_catalog = SVV_COLUMNS.table_catalog\n" +
                "    AND SVV_TABLES.table_schema = SVV_COLUMNS.table_schema\n" +
                "    AND SVV_TABLES.table_name = SVV_COLUMNS.table_name\n" +
                "WHERE SVV_COLUMNS.TABLE_CATALOG = 'analytics'\n" +
                "AND SVV_COLUMNS.TABLE_SCHEMA = 'backend'";

        return  jdbcTemplate.query(sql, new BeanPropertyRowMapper(SvvColumn.class));
    }

    private void registerEntities(List<SvvColumn> columns) throws Exception {
        // Create Schema Entities
        columns.stream()
                .collect(groupingBy(c -> new AbstractMap.SimpleEntry<>(c.getSchema(), c.getTable_name())))
                .forEach((k, v) -> {
                    try {
                        AtlasEntity database = registerDatabase(k.getKey(), k.getKey(), "Data Hub", redshiftUrl);
                        String databaseName = k.getKey();
                        String tableName = v.get(0).getTable_name();

                        String tableQualifiedName = new StringBuilder()
                                .append(databaseName)
                                .append(".")
                                .append(tableName)
                                .toString();

                        AtlasEntity tableEntity = registerTable(v.get(0).getTable_name(), v.get(0).getTable_remarks(), database, "Data Hub", v.get(0).getTable_type(),
                                tableQualifiedName,
                                DIMENSION_CLASSIFICATION);


                        AtlasEntity storageDesc = registerStorageDescriptor(redshiftUrl, "TextInputFormat", "TextOutputFormat", true, tableEntity);

                        List<AtlasEntity> columnsEntities = v.stream()
                                .map(i -> {
                                    try {
                                        String columnName = i.getColumn_name();
                                        String qualifiedName = new StringBuilder()
                                                .append(databaseName)
                                                .append(".")
                                                .append(tableName)
                                                .append(".")
                                                .append(columnName)
                                                .toString();
                                        return registerColumn(i.getColumn_name(), i.getData_type(), i.getColumn_remarks(), qualifiedName, tableEntity);
                                    } catch (AtlasServiceException e) {
                                        e.printStackTrace();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    return null;
                                })
                                .collect(toList());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    AtlasEntity registerDatabase(String name, String description, String owner, String locationUri, String... classificationNames) throws Exception {
        AtlasEntity database = findDatabase(name);

        if(database == null) {
            database =  createDatabase(name, description, owner, locationUri, classificationNames);
        }
        return database;
    }

    AtlasEntity registerColumn(String name, String dataType, String comment, String qualifiedName, AtlasEntity tableEntity, String... classificationNames) throws Exception {
        AtlasEntity column = findColumn(qualifiedName);

        if(column == null) {
            column =  createColumn(name, dataType, comment, qualifiedName, tableEntity, classificationNames);
        }
        return column;
    }


    AtlasEntity registerTable(String name, String description, AtlasEntity database, String owner, String tableType,
                            String qualifiedName, String... classificationNames) throws Exception {
        AtlasEntity table = findTable(qualifiedName);

        if(table == null) {
            table =  createTable(name, description, database, owner, tableType, qualifiedName, classificationNames);
        }
        return table;
    }

    AtlasEntity registerStorageDescriptor(String location, String inputFormat, String outputFormat, boolean compressed, AtlasEntity tblEntity) throws Exception {
        AtlasEntity storageDescriptor = findStorageDescriptor(location);

        if(storageDescriptor == null) {
            storageDescriptor =  createStorageDescriptor(location, inputFormat, outputFormat, compressed, tblEntity);
        }
        return storageDescriptor;

    }

    private AtlasEntity findStorageDescriptor(String location) throws AtlasServiceException {
        return findEntity(STORAGE_DESC_TYPE, Collections.singletonMap("location", location));
    }

    private AtlasEntity findDatabase(String dbName) throws AtlasServiceException {
        return findEntity(DATABASE_TYPE, Collections.singletonMap("name", dbName));
    }

    private AtlasEntity findColumn(String qualifiedName) throws AtlasServiceException {
        return findEntity(COLUMN_TYPE, Collections.singletonMap(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName));
    }

    private AtlasEntity findTable(String qualifiedName) throws AtlasServiceException {
        return findEntity(TABLE_TYPE, Collections.singletonMap(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName));
    }

    private AtlasEntity findEntity(String typeName, Map<String, String> uniqAttributes) throws AtlasServiceException {
        AtlasEntityWithExtInfo ret = null;

        try {
            ret = atlasClientV2.getEntityByAttribute(typeName, uniqAttributes);
        } catch (AtlasServiceException e) {
            if(e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }

            throw e;
        }

        clearRelationshipAttributes(ret);

        return ret.getEntity();
    }

    private void clearRelationshipAttributes(AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    private void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    private void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }

    AtlasEntity createDatabase(String name, String description, String owner, String locationUri, String... classificationNames) throws Exception {
        AtlasEntity entity = new AtlasEntity(DATABASE_TYPE);

        // set attributes
        entity.setAttribute("name", name);
        entity.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, name + CLUSTER_SUFFIX);
        entity.setAttribute("description", description);
        entity.setAttribute("owner", owner);
        entity.setAttribute("locationuri", locationUri);
        entity.setAttribute("createTime", System.currentTimeMillis());

        // set classifications
        entity.setClassifications(toAtlasClassifications(classificationNames));

        return createInstance(entity);
    }

    AtlasEntity createTable(String name, String description, AtlasEntity database, String owner, String tableType,
                            String qualifiedName, String... classificationNames) throws Exception {

        AtlasEntity tblEntity = new AtlasEntity(TABLE_TYPE);

        // set attributes
        tblEntity.setAttribute("name", name);
        tblEntity.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName);
        tblEntity.setAttribute("description", description);
        tblEntity.setAttribute("owner", owner);
        tblEntity.setAttribute("tableType", tableType);
        tblEntity.setAttribute("createTime", System.currentTimeMillis());
        tblEntity.setAttribute("lastAccessTime", System.currentTimeMillis());
        tblEntity.setAttribute("retention", System.currentTimeMillis());

        tblEntity.setRelationshipAttribute("db", toAtlasRelatedObjectId(database));

        // set classifications
        tblEntity.setClassifications(toAtlasClassifications(classificationNames));

        return createInstance(tblEntity);
    }

    AtlasEntity createStorageDescriptor(String location, String inputFormat, String outputFormat, boolean compressed, AtlasEntity tblEntity) throws Exception {
        AtlasEntity ret = new AtlasEntity(STORAGE_DESC_TYPE);

        ret.setAttribute("name", "sd:" + location);
        ret.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, "sd:" + location + CLUSTER_SUFFIX);
        ret.setAttribute("location", location);
        ret.setAttribute("inputFormat", inputFormat);
        ret.setAttribute("outputFormat", outputFormat);
        ret.setAttribute("compressed", compressed);

        ret.setRelationshipAttribute("table", toAtlasRelatedObjectId(tblEntity));

        return createInstance(ret);
    }

    AtlasEntity createColumn(String name, String dataType, String comment, String qualifiedName, AtlasEntity tableEntity, String... classificationNames) throws Exception {
        AtlasEntity ret = new AtlasEntity(COLUMN_TYPE);

        // set attributes
        ret.setAttribute("name", name);
        ret.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName);
        ret.setAttribute("dataType", dataType);
        ret.setAttribute("comment", comment);

         // set relationship
        ret.setRelationshipAttribute("table", toAtlasRelatedObjectId(tableEntity));

        // set classifications
        ret.setClassifications(toAtlasClassifications(classificationNames));

        return createInstance(ret);
    }

    private List<AtlasClassification> toAtlasClassifications(String[] classificationNames) {
        List<AtlasClassification> ret             = new ArrayList<>();
        List<String>              classifications = asList(classificationNames);

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (String classificationName : classifications) {
                ret.add(new AtlasClassification(classificationName));
            }
        }

        return ret;
    }

    private AtlasEntity createInstance(AtlasEntity entity) throws Exception {
        return createInstance(new AtlasEntityWithExtInfo(entity));
    }

    private AtlasEntity createInstance(AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        AtlasEntity             ret      = null;
        EntityMutationResponse  response = atlasClientV2.createEntity(entityWithExtInfo);
        List<AtlasEntityHeader> entities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(entities)) {
            AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());

            ret = getByGuidResponse.getEntity();

            System.out.println("Created entity of type [" + ret.getTypeName() + "], guid: " + ret.getGuid());
        }

        return ret;
    }
}
