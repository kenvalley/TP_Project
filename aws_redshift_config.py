
def aws_configure():

    import boto3
    import json
    import configparser
    from botocore.exceptions import ClientError
    import time

    # def conf():
    try:
        config = configparser.ConfigParser()
        config.read_file(open('encrypt.cfg'))

        KEY                    = config.get('AWS','KEY')
        SECRET                 = config.get('AWS','SECRET')

        DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

        DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        DWH_DB                 = config.get("DWH","DWH_DB")
        DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        DWH_PORT               = config.get("DWH","DWH_PORT")

        DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

        #def clients():
        ec2 = boto3.resource('ec2',
                region_name="eu-west-2",
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET
                )
        s3 = boto3.resource('s3',
                region_name="eu-west-2",
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET
            )
            
        iam = boto3.client('iam',aws_access_key_id=KEY,
                aws_secret_access_key=SECRET,
                region_name='eu-west-2'
            )
            
        redshift = boto3.client('redshift',
                region_name="eu-west-2",
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET
                )

        #1.1 Create the role 
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
            )    
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                pass
            else:
                print("Unexpected error: %s" % e)

        #Attaching policy    
        print("1.2 Attaching Policy")

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(roleArn)

    except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print("User already exists")
                pass
            else:
                print("Unexpected error: %s" % e)

    #def create_redshift_cluster():
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
        time.sleep(120)
        ClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        #print(ClusterProps['ClusterStatus'])
        #print(ClusterProps['Endpoint']) #Obtain endpoint address
        #endpoint = ClusterProps['Endpoint']['Address']
        #print(ClusterProps['Endpoint']['Address'])


    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            #print("Cluster already exists")
            ClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            #print(ClusterProps['Endpoint']) #Obtain endpoint address
            endpoint = ClusterProps['Endpoint']['Address']
            print(endpoint)
            #print(ClusterProps['Endpoint']['Address'])
            
            pass
        else:
            print("Unexpected error: %s" % e)

    

    # Create Security group inbound rule
    # Open an incoming  TCP port to access the cluster ednpoint
    try:
        vpc = ec2.Vpc(id=ClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
    
        defaultSg.authorize_ingress(
            GroupName= 'default',  
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',  
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print('already exists') #Obtain endpoint address
            pass
        else:
            print("Unexpected error: %s" % e)

    return endpoint


    #create_redshift_cluster()


#aws_configure()