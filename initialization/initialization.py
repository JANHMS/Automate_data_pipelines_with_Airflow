import pandas as pd
import boto3
import configparser
import json


def main():

    print("Get config params")
    config_file_path = 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    CLUSTER_IDENTIFIER     = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    CLUSTER_TYPE           = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    NUM_NODES              = config.get("CLUSTER","DWH_NUM_NODES")
    NODE_TYPE              = config.get("CLUSTER","DWH_NODE_TYPE")
    REGION                 = config.get("CLUSTER","DWH_REGION")

    IAM_ROLE_NAME          = config.get("CLUSTER","DWH_IAM_ROLE_NAME")

    DB_NAME                = config.get("CLUSTER","DWH_DB")
    DB_USER                = config.get("CLUSTER","DWH_DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DWH_DB_PASSWORD")
    DB_PORT                = config.get("CLUSTER","DWH_PORT")

    print("Creating iam and redshift clients")
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    print("Creating a new IAM Role")
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )
    except Exception as e:
        print(e)


    print("Attaching Policy")
    try:
        iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)

    print("Get the IAM role ARN")
    try:
        ROLE_ARN = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
        print(ROLE_ARN)
    except Exception as e:
        print(e)

    print("Update config file with IAM role ARN")
    with open(config_file_path, "w") as config_file:
        config.set("CLUSTER", "DWH_ROLE_ARN", ROLE_ARN)
        config.write(config_file)

    print("Create cluster")
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[ROLE_ARN]
        )
    except Exception as e:
        print(e)

    print("Wait until cluster is created")
    while True:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = cluster_props["ClusterStatus"]
        if cluster_status == "available":
            print("cluster created")
            break

    print("Get cluster endpoint")
    ENDPOINT = cluster_props['Endpoint']['Address']
    print(ENDPOINT)

    print("Update config file with cluster endpoint")
    with open(config_file_path, "w") as config_file:
        config.set("CLUSTER", "DWH_ENDPOINT", ENDPOINT)
        config.write(config_file)

    print("Open an incoming  TCP port to access the cluster ednpoint. If rule already exists, exception generated.")
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
