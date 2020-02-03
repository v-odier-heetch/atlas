package org.apache.atlas.redshift.model;

import lombok.Data;

@Data
public class SvvColumn {
    private String table_type;
    private String table_remarks;
    private String catalog;
    private String schema;
    private String table_name;
    private String column_name;
    private Integer ordinal_position;
    private String column_default;
    private String is_nullable;
    private String data_type;
    private Integer character_maximum_length;
    private Integer numeric_precision;
    private Integer numeric_precision_radix;
    private Integer numeric_scale;
    private Integer datetime_precision;
    private String interval_type;
    private String interval_precision;
    private String character_set_catalog;
    private String character_set_schema;
    private String character_set_name;
    private String collation_catalog;
    private String collation_schema;
    private String collation_name;
    private String domain_name;
    private String column_remarks;
}
