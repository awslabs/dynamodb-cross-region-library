fs = require('fs')
negate = (condition)  ->
        {
            "Fn::Not": [
                condition
            ]
        }

equals = (value1, value2) ->
        {
            "Fn::Equals": [
                value1,
                value2
            ]
        }

ref = (name) ->
	{
		Ref: name
	}

find = (map, key1, key2) ->
	{
		"Fn::FindInMap": [
			map,
			key1,
			key2
		]
	}

base64 = (object) ->
	{
		"Fn::Base64": object
	}

getAtt = (resource, attrName) ->
	{
		"Fn::GetAtt": [
			resource,
			attrName
		]
	}

join = (array) ->
	{
		"Fn::Join": [
			"",
			array
		]
	}

getAZs = () ->
	{
		"Fn::GetAZs": ref("AWS::Region")
	}

setOnlyIf = (name, param) ->
	{
		"Fn::If": [
			name
			param
			ref("AWS::NoValue")
		]
	}

getDynamoDBEndpoint = (region) ->
        join [  "dynamodb."
                region
                ".amazonaws.com"
        ]

getKCLTableName = (metadataRegion, metadataTable) ->
        join [
                "DynamoDBCrossRegionReplication_"
                metadataRegion
                "_"
                metadataTable
        ]

getStreamsArn = (account, region, name) ->
	join [
		"arn:aws:dynamodb:"
		region
		":"
		account
		":table/"
		name
                "/"
                "stream/"
                "*"
	]

getTableArn = (account, region, name) ->
	join [
		"arn:aws:dynamodb:"
		region
		":"
		account
		":table/"
		name
	]

replaceReferences = (line) ->
	lineArray = replaceReferenceRecursive([], line)
	if typeof lineArray[lineArray.length - 1] == 'string'
		lineArray[lineArray.length - 1] += '\n'
	else
		# The last element in lineArray is a referenced variable (== object).
		# We add newline character as a new element.
		lineArray.push '\n'
	lineArray

replaceReferenceRecursive = (array, string) ->
	if found = string.match(/(.*)\@\{(.+)\}(.*)/)
		# A referenced variable is found.
		# Recursively call until we do not find any more referenced variable
		replaceReferenceRecursive(array, found[1])
		# Replace the variable with {Ref: "ReferencedVariable"}
		array.push eval(found[2])
		# Push string after the variable as an element if any
		array.push found[3] if (found[3] && found[3].length > 0)
		array
	else
		# No more referenced variable to replace.
		# We should add newline at the end.
		array.push string
		array

getResource = (path) ->
	content = fs.readFileSync(path, {encoding: 'UTF-8'})
	array = []
	for line in content.split('\n')
		# line may contain @{ReferencedVariable}s. Replace them with 
		# {Ref: "ReferencedVariable"}s and add as separamete elements in the array.
		array.push.apply(array, replaceReferences(line))
	join(array)

	


