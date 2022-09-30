import boto3
import configparser


def main():

    print("Get config params")
    config_file_path = 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    CLUSTER_IDENTIFIER     = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    REGION                 = config.get("CLUSTER","DWH_REGION")
    IAM_ROLE_NAME          = config.get("CLUSTER","DWH_IAM_ROLE_NAME")



    print("Create redshift, iam clients")
    try:
        redshift = boto3.client('redshift',
                           region_name=REGION,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
        iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=REGION
                  )
    except Exception as e:
        print(e)

    print("delete redshift cluster")
    try:
        redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)

    print("Wait until cluster is deleted")


    while True:
        try:
            cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        except Exception as e:
            print("cluster deleted")
            break

    print("delete IAM role")
    try:
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=IAM_ROLE_NAME)
    except Exception as e:
        print(e)



if __name__ == "__main__":
    main()
