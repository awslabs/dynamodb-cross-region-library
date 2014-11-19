util = require('./util')
nconf = require('./mynconf')

tables = [
    {
        TableName: nconf.get('TABLE_COMMON')
        AttributeDefinitions: [
            { AttributeName: 'device_id', AttributeType: 'S' }
            { AttributeName: 'time', AttributeType: 'S' }
        ]
        KeySchema: [
            { AttributeName: 'device_id', KeyType: 'HASH' }
            { AttributeName: 'time', KeyType: 'RANGE' }            
        ]
        ProvisionedThroughput:
            ReadCapacityUnits: nconf.get('READ_CAPACITY')
            WriteCapacityUnits: nconf.get('WRITE_CAPACITY')
    }
]

for table in tables
    util.createTable(table, nconf.get('delete_table_if_exists') 
        (error) ->
            console.error error
        (tableName) ->
            console.log "Table #{tableName} is ready"
    )
