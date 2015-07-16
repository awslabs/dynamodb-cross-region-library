# Constants used in the source cloud formation coffeescript
DynamoDBReplicationCoordinatorApplicationSourceS3Bucket = "dynamodb-cross-region"
DynamoDBReplicationConsoleSourceS3Bucket = "dynamodb-cross-region-replication-console"
DynamoDBReplicationCoordinatorApplicationSourceS3Key = "DynamoDBReplicationCoordinatorApplicationBundle.zip"
DynamoDBReplicationCoordinatorJarS3Key = "DynamoDBReplicationCoordinator.jar"
DynamoDBReplicationCoordinatorServerJarS3Key = "DynamoDBReplicationServer.jar"
DynamoDBReplicationCoordinatorServerHttpPath = "/"
DynamoDBReplicationCoordinatorServerMaxRetries = "1"

Map =
	AWSRegionArch2AMI:
		"us-east-1": {
			PV64: "ami-50842d38"
			HVM64 : "ami-08842d60"
			HVMG2 : "ami-3a329952"
		}
		"us-west-2" : {
			PV64 : "ami-af86c69f"
			HVM64 : "ami-8786c6b7"
			HVMG2 : "ami-47296a77"
		}
		"us-west-1" : {
			PV64 : "ami-c7a8a182"
			HVM64 : "ami-cfa8a18a"
			HVMG2 : "ami-331b1376"
		}
		"eu-west-1" : {
			PV64 : "ami-aa8f28dd"
			HVM64 : "ami-748e2903"
			HVMG2 : "ami-00913777"
		}
		"ap-southeast-1" : {
			PV64 : "ami-20e1c572"
			HVM64 : "ami-d6e1c584"
			HVMG2 : "ami-fabe9aa8"
		}
		"ap-northeast-1" : {
			PV64 : "ami-21072820"
			HVM64 : "ami-35072834"
			HVMG2 : "ami-5dd1ff5c"
		}
		"ap-southeast-2" : {
			PV64 : "ami-8b4724b1"
			HVM64 : "ami-fd4724c7"
			HVMG2 : "ami-e98ae9d3"
		}
		"sa-east-1" : {
			PV64 : "ami-9d6cc680"
			HVM64 : "ami-956cc688"
			HVMG2 : "NOT_SUPPORTED"
		}
		"cn-north-1" : {
			PV64 : "ami-a857c591"
			HVM64 : "ami-ac57c595"
			HVMG2 : "NOT_SUPPORTED"
		}
		"eu-central-1" : {
			PV64 : "ami-a03503bd"
			HVM64 : "ami-b43503a9"
			HVMG2 : "ami-b03503ad"
		}

	AWSInstanceType2Arch:
		"t1.micro" : {
			Arch : "PV64"
		}
		"t2.micro" : {
			Arch : "HVM64"
		}
		"t2.small" : {
			Arch : "HVM64"
		}
		"t2.medium" : {
			Arch : "HVM64"
		}
		"m1.small" : {
			Arch : "PV64"
		}
		"m1.medium" : {
			Arch : "PV64"
		}
		"m1.large" : {
			Arch : "PV64"
		}
		"m1.xlarge" : {
			Arch : "PV64"
		}
		"m2.xlarge" : {
			Arch : "PV64"
		}
		"m2.2xlarge" : {
			Arch : "PV64"
		}
		"m2.4xlarge" : {
			Arch : "PV64"
		}
		"m3.medium" : {
			Arch : "HVM64"
		}
		"m3.large" : {
			Arch : "HVM64"
		}
		"m3.xlarge" : {
			Arch : "HVM64"
		}
		"m3.2xlarge" : {
			Arch : "HVM64"
		}
		"c1.medium" : {
			Arch : "PV64"
		}
		"c1.xlarge" : {
			Arch : "PV64"
		}
		"c3.large" : {
			Arch : "HVM64"
		}
		"c3.xlarge" : {
			Arch : "HVM64"
		}
		"c3.2xlarge" : {
			Arch : "HVM64"
		}
		"c3.4xlarge" : {
			Arch : "HVM64"
		}
		"c3.8xlarge" : {
			Arch : "HVM64"
		}
		"c4.large" : {
			Arch : "HVM64"
		}
		"c4.xlarge" : {
			Arch : "HVM64"
		}
		"c4.2xlarge" : {
			Arch : "HVM64"
		}
		"c4.4xlarge" : {
			Arch : "HVM64"
		}
		"c4.8xlarge" : {
			Arch : "HVM64"
		}
		"g2.2xlarge" : {
			Arch : "HVMG2"
		}
		"r3.large" : {
			Arch : "HVM64"
		}
		"r3.xlarge" : {
			Arch : "HVM64"
		}
		"r3.2xlarge" : {
			Arch : "HVM64"
		}
		"r3.4xlarge" : {
			Arch : "HVM64"
		}
		"r3.8xlarge" : {
			Arch : "HVM64"
		}
		"i2.xlarge" : {
			Arch : "HVM64"
		}
		"i2.2xlarge" : {
			Arch : "HVM64"
		}
		"i2.4xlarge" : {
			Arch : "HVM64"
		}
		"i2.8xlarge" : {
			Arch : "HVM64"
		}
		"hi1.4xlarge" : {
			Arch : "HVM64"
		}
		"hs1.8xlarge" : {
			Arch : "HVM64"
		}
		"cr1.8xlarge" : {
			Arch : "HVM64"
		}
		"cc2.8xlarge" : {
			Arch : "HVM64"
		}

Lists =
	AWSRegions: [
		"us-east-1"
		"us-west-2"
		"us-west-1"
		"eu-west-1"
		"eu-central-1"
		"ap-southeast-1"
		"ap-northeast-1"
		"ap-southeast-2"
		"sa-east-1"
		"cn-north-1"
	]

	AWSInstanceTypes: [
		"t1.micro"
		"t2.micro"
		"t2.small"
		"t2.medium"
		"m1.small"
		"m1.medium"
		"m1.large"
		"m1.xlarge"
		"m2.xlarge"
		"m2.2xlarge"
		"m2.4xlarge"
		"m3.medium"
		"m3.large"
		"m3.xlarge"
		"m3.2xlarge"
		"c1.medium"
		"c1.xlarge"
		"c3.large"
		"c3.xlarge"
		"c3.2xlarge"
		"c3.4xlarge"
		"c3.8xlarge"
		"c4.large"
		"c4.xlarge"
		"c4.2xlarge"
		"c4.4xlarge"
		"c4.8xlarge"
		"g2.2xlarge"
		"r3.large"
		"r3.xlarge"
		"r3.2xlarge"
		"r3.4xlarge"
		"r3.8xlarge"
		"i2.xlarge"
		"i2.2xlarge"
		"i2.4xlarge"
		"i2.8xlarge"
		"hi1.4xlarge"
		"hs1.8xlarge"
		"cr1.8xlarge"
		"cc2.8xlarge"
		"cg1.4xlarge"
	]

