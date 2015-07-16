Description = "DynamoDB Cross Region Replication Coordinator"

Parameters =
# Debugging purposes only.
  KeyName:
    Description: "(Optional) Name of an existing EC2 KeyPair to enable SSH access to the instance"
    # not using "AWS::EC2::KeyPair::KeyName" because it validates and ensures this parameter is non-empty
    Type: "String"
    Default: ""
    MinLength: "0"
    MaxLength: "255"
    AllowedPattern: "[\\x20-\\x7E]*"
    ConstraintDescription: "can contain only ASCII characters."

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

Conditions =
    KeyNameExists: negate(equals(ref("KeyName"), ""))

Mappings =
  AWSInstanceType2Arch: Map.AWSInstanceType2Arch
  AWSRegionArch2AMI: Map.AWSRegionArch2AMI

Resources =
  EcsInstanceLc:
    Type : "AWS::AutoScaling::LaunchConfiguration",
    Properties:
      ImageId : find("AWSRegionArch2AMI", ref("AWS::Region"), find("AWSInstanceType2Arch", ref("EcsInstanceType"), "Arch"))
      InstanceType : ref("EcsInstanceType")
      IamInstanceProfile: ref("EcsInstanceProfile")
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
      SecurityGroupIngress : setOnlyIf("KeyNameExists", [
            IpProtocol : "tcp"
            FromPort : "22"
            ToPort : "22"
            CidrIp : "0.0.0.0/0"
        ])

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
