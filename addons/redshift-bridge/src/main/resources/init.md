# Create Custom Atlas ResearchPaperAccessDataset Type:

```
curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin 'http://yellow.hdp.com:21000/api/atlas/types' -d @atlas_type_ResearchPaperDataSet.json
```

atlas_type_ResearchPaperDataSet.json
```
{
    "enumTypes": [],
    "structTypes": [],
    "traitTypes": [],
    "classTypes": [{
        "superTypes": ["DataSet"],
        "hierarchicalMetaTypeName": "org.apache.atlas.typesystem.types.ClassType",
        "typeName": "ResearchPaperAccessDataset",
        "typeDescription": null,
        "attributeDefinitions": [{
            "name": "resourceSetID",
            "dataTypeName": "int",
            "multiplicity": "required",
            "isComposite": false,
            "isUnique": false,
            "isIndexable": true,
            "reverseAttributeName": null
        },
        {
            "name": "researchPaperGroupName",
            "dataTypeName": "string",
            "multiplicity": "required",
            "isComposite": false,
            "isUnique": false,
            "isIndexable": true,
            "reverseAttributeName": null
        },
        {
            "name": "createTime",
            "dataTypeName": "date",
            "multiplicity": "required",
            "isComposite": false,
            "isUnique": false,
            "isIndexable": true,
            "reverseAttributeName": null
        }]
    }]
}
```