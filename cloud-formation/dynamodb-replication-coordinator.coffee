Description = "DynamoDB Cross Region Replication Coordinator"

Parameters =
# Debugging purposes only. Note that prepare_console_link.sh may fail when enabling this parameter in elastic beanstalk. 
# Add the parameter KeyName in CloudFormation UpdateStack call in ./coordinatorContainer/scripts/prepare_console_link.sh 
# if needed. 
  KeyName:
    Description: "(Optional) Name of an existing EC2 KeyPair to enable SSH access to the instance"
    # not using "AWS::EC2::KeyPair::KeyName" because it validates and ensures this parameter is non-empty
    Type: "String"
    Default: ""
    MinLength: "0"
    MaxLength: "255"
    AllowedPattern: "[\\x20-\\x7E]*"
    ConstraintDescription: "can contain only ASCII characters."

  CoordinatorInstanceType:
    Description: "DynamoDBReplicationCoordinator EC2 instance type"
    Type: "String"
    # To make it compatible with EC2 classic user, it defaults to t1.micro. If we can detect whether default VPC exists, 
    # we should make it conditional and use t2.micro for VPC-by-default users. 
    Default : "t1.micro"
    AllowedValues : Lists.AWSInstanceTypes
    ConstraintDescription : "must be a valid EC2 instance type."

  MetadataTableName:
    Description: "The name of the metadata table"
    Type: "String"
    Default : "DynamoDBReplicationCoordinatorMetadata"
    ConstraintDescription : "must be a valid DynamoDB table name"

  MetadataTableRegion:
    Description: "The region of the metadata table"
    Type: "String"
    Default : "us-east-1"
    AllowedValues : Lists.AWSRegions
    ConstraintDescription : "must be a valid DynamoDB region"
  
  EcsInstanceType:
    Type: "String"
    Description: "EC2 instance type for Elastic Container Service (used to host replication processes)"
    Default: "t1.micro"
    AllowedValues : Lists.AWSInstanceTypes
    ConstraintDescription : "must be a valid EC2 instance type."

  EcsClusterSize:
    Type: "Number"
    Description: "Maximum size and initial Desired Capacity of ECS Auto Scaling Group"
    Default : "1"

  SQSUrl:
    Type: "String"
    Description: "The SQS Worker Queue URL where messages from the console are sent to, do not edit as it is automatically generated."
    Default: "Do not fill in. This field will be automatically populated."

  SQSDeadLetterUrl:
    Type: "String"
    Description: "The SQS Worker Dead Letter Queue URL where returned error messages are sent to, do not edit as it is automatically generated."
    Default: "Do not fill in. This field will be automatically populated."

Conditions =
    KeyNameExists: negate(equals(ref("KeyName"), ""))

Mappings =
  AWSInstanceType2Arch: Map.AWSInstanceType2Arch
  AWSRegionArch2AMI: Map.AWSRegionArch2AMI

Resources =
  DynamoDBReplicationCoordinatorApplication:
    Type: "AWS::ElasticBeanstalk::Application"
    Description: "DynamoDB Cross Region Replication Coordinator Worker Application"

  CoordinatorVersion:
    Type: "AWS::ElasticBeanstalk::ApplicationVersion"
    Properties:
      ApplicationName: ref("DynamoDBReplicationCoordinatorApplication")
      SourceBundle:
        S3Bucket: join([DynamoDBReplicationCoordinatorApplicationSourceS3Bucket, "-", ref("AWS::Region")])
        S3Key: DynamoDBReplicationCoordinatorApplicationSourceS3Key
  
  CoordinatorConfigurationTemplate:
    Type: "AWS::ElasticBeanstalk::ConfigurationTemplate"
    Properties:
      ApplicationName: ref("DynamoDBReplicationCoordinatorApplication")
      OptionSettings: [
        {
          Namespace: "aws:elasticbeanstalk:environment"
          OptionName: "EnvironmentType"
          Value: "SingleInstance"
        }
        {
           Namespace : "aws:autoscaling:launchconfiguration",
           OptionName : "IamInstanceProfile",
           Value : ref("DynamoDBReplicationCoordinatorProfile")
        }
        {
           Namespace : "aws:autoscaling:launchconfiguration",
           OptionName : "InstanceType",
           Value : ref("CoordinatorInstanceType")
        }
        # Debugging purposes only
        #{
        #  Namespace : "aws:autoscaling:launchconfiguration",
        #  OptionName : "EC2KeyName",
        #  Value : ref("KeyName")
        #}
        {
           Namespace : "aws:elasticbeanstalk:sqsd",
           OptionName : "HttpPath",
           Value : DynamoDBReplicationCoordinatorServerHttpPath
        }
        {
           Namespace : "aws:elasticbeanstalk:sqsd",
           OptionName : "MaxRetries",
           Value : DynamoDBReplicationCoordinatorServerMaxRetries
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "SQS_URL",
           Value : ref("SQSUrl")
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "SQS_DEAD_LETTER_URL",
           Value : ref("SQSDeadLetterUrl")
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "AWS_ACCOUNT_ID",
           Value : ref("AWS::AccountId")
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "SOURCE_BUCKET",
           Value : DynamoDBReplicationCoordinatorApplicationSourceS3Bucket
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "COORDINATOR_JAR",
           Value : DynamoDBReplicationCoordinatorJarS3Key
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "COORDINATOR_SERVER_JAR",
           Value : DynamoDBReplicationCoordinatorServerJarS3Key
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "METADATA_TABLE_ENDPOINT",
           Value : getDynamoDBEndpoint(ref("MetadataTableRegion"))
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "METADATA_TABLE_NAME",
           Value : ref("MetadataTableName")
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "CURRENT_REGION",
           Value : ref("AWS::Region")
        }
        {
           Namespace : "aws:elasticbeanstalk:application:environment",
           OptionName : "CLOUDFORMATION_STACK_NAME",
           Value : ref("AWS::StackName")
        }
      ]
      SolutionStackName: "64bit Amazon Linux 2015.03 v1.4.3 running Docker 1.6.2"

  DynamoDBReplicationCoordinatorApplicationEnvironment:
    Type: "AWS::ElasticBeanstalk::Environment"
    Description: "DynamoDB Cross Region Replication Coordinator Worker Environment"
    Properties:
      ApplicationName: ref("DynamoDBReplicationCoordinatorApplication")
      TemplateName: ref("CoordinatorConfigurationTemplate")
      VersionLabel: ref("CoordinatorVersion")
      Tier:
        Name: "Worker"
        Type: "SQS/HTTP"
        Version: "1.1"

  DynamoDBReplicationCoordinatorProfile:
    Type: "AWS::IAM::InstanceProfile"
    Properties:
      Path : "/dynamodb-coordinator/"
      Roles : [ ref("DynamoDBReplicationCoordinatorRole") ]

  DynamoDBReplicationCoordinatorRole:
    Type: "AWS::IAM::Role",
    Properties:
      AssumeRolePolicyDocument :
        Version : "2012-10-17"
        Statement : [
          Effect : "Allow"
          Principal :
            Service : ["ec2.amazonaws.com"]
          Action : ["sts:AssumeRole"]
        ]
      Path : "/dynamodb-coordinator/"
      Policies : [
        PolicyName : "DynamoDBReplicationCoordinatorPolicy"
        PolicyDocument :
          Version : "2012-10-17"
          Statement : [
            {
              Effect : "Allow"
              Action : "dynamodb:*" #TODO: We should limit action(s) needed to read the source table
              Resource : [
                getTableArn(ref("AWS::AccountId"), ref("MetadataTableRegion"), ref("MetadataTableName"))
                getStreamsArn(ref("AWS::AccountId"), ref("MetadataTableRegion"), ref("MetadataTableName"))
                getTableArn(ref("AWS::AccountId"), ref("MetadataTableRegion"), getKCLTableName(ref("MetadataTableRegion"), ref("MetadataTableName")))
              ]
            }
            {
              Effect : "Allow"
              Action : [ 
                # For coordinator to create table when a replica table does not exist
                "dynamodb:DescribeTable",
                "dynamodb:CreateTable",
                # For updating CloudFormation output parameters
                "cloudformation:UpdateStack",
                "cloudformation:GetTemplate",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:DescribeStackResource",
		"cloudformation:ListStackResources",
                "cloudformation:CancelUpdateStack",
                #TODO: We should limit action(s) needed to only relevant actions
                "ec2:*",
                "ecs:*",
                "autoscaling:*",
                "sqs:*",
                "cloudwatch:*",
                "elasticbeanstalk:*"
                "s3:*"
              ]
              Resource : [
                "*"
              ]
            }
            { # As recommended on http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/AWSHowTo.cloudwatchlogs.html 
              Effect: "Allow",
              Action: [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:GetLogEvents",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutRetentionPolicy"
              ],
              Resource: [
                join(["arn:aws:logs:", ref("AWS::Region"), ":*:*"])
              ]
            }
          ]
        ]

  EcsInstanceLc:
    Type : "AWS::AutoScaling::LaunchConfiguration",
    Properties:
      ImageId : find("AWSRegionArch2AMI", ref("AWS::Region"), find("AWSInstanceType2Arch", ref("EcsInstanceType"), "Arch"))
      InstanceType : ref("EcsInstanceType")
      IamInstanceProfile: ref("EcsInstanceProfile")
      # Debugging purposes
      KeyName : setOnlyIf("KeyNameExists", ref("KeyName"))
      SecurityGroups : [ ref("EcsInstanceSecurityGroup") ]
      UserData : base64(getResource('./resources/create_cluster_and_setup_ecs.sh'))

  EcsInstanceAsg:
    Type : "AWS::AutoScaling::AutoScalingGroup",
    Properties:
      AvailabilityZones: getAZs()
      LaunchConfigurationName: ref("EcsInstanceLc")
      MinSize : "1",
      MaxSize : ref("EcsClusterSize")
      DesiredCapacity: ref("EcsClusterSize")
      Tags: [
        { "Key" : "Name", "Value" : join([ "ECS Instance - ", ref("AWS::StackName")]), "PropagateAtLaunch" : "true" }
      ]

  EcsInstanceSecurityGroup:
    Type: "AWS::EC2::SecurityGroup"
    Properties:
      GroupDescription : "Security group for DynamoDB replication worker"

  EcsInstanceProfile:
    Type: "AWS::IAM::InstanceProfile"
    Properties:
      Path : "/dynamodb-replication-worker/"
      Roles : [ ref("EcsInstanceRole") ]

  EcsInstanceRole:
    Type: "AWS::IAM::Role",
    Properties:
      AssumeRolePolicyDocument :
        Version : "2012-10-17"
        Statement : [
          Effect : "Allow"
          Principal :
            Service : ["ec2.amazonaws.com"]
          Action : ["sts:AssumeRole"]
        ]
      Path : "/dynamodb-replication-worker/"
      Policies : [
        PolicyName : "DynamoDBReplicationWorkerPolicy"
        PolicyDocument :
          Version : "2012-10-17"
          Statement : [
            {
              Effect : "Allow"
              Action : [ #TODO: We should limit action(s) needed to only relevant actions
                "dynamodb:*"
                "cloudwatch:*"
                "ecs:*"
              ]
              Resource : [
                "*"
              ]
            }
            { # As recommended on http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/AWSHowTo.cloudwatchlogs.html 
              Effect: "Allow",
              Action: [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:GetLogEvents",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutRetentionPolicy"
              ],
              Resource: [
                join(["arn:aws:logs:", ref("AWS::Region"), ":*:*"])
              ]
            }
          ]
        ]

Outputs =
  ReplicationConsoleURL:
    Value: join(["https://", DynamoDBReplicationConsoleSourceS3Bucket, ".s3.amazonaws.com/index.html#/?", "AccountId=", ref("AWS::AccountId"), "&SQSUrl=", ref("SQSUrl"), "&SQSDeadLetterUrl=", ref("SQSDeadLetterUrl"), "&SQSRegion=", ref("AWS::Region"), "&MetadataTableName=", ref("MetadataTableName"), "&MetadataTableRegion=", ref("MetadataTableRegion")])
    Description: "URL of the DynamoDB Replication Console"

