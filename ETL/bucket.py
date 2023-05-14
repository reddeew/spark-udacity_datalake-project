import boto3
import configparser
config= configparser.ConfigParser()
config.read('aws.cfg')
target_bucket_name = config['IAC']['Target_bucket_name']
region=config['IAC']['region']
s3 = boto3.client('s3')


def Create_Bucket():
 try :
    #print(target_bucket_name)
    #response = s3.create_bucket(Bucket=target_bucket_name ,CreateBucketConfiguration={'LocationConstraint': region})
    response = s3.create_bucket(Bucket=target_bucket_name)
    print(response)
    #response = response.[ResponseMetadata][HTTPStatusCode]
 except Exception as e:
    print(e.response['Error']['code'])
    print(e.response['Error']['Message'])
def Delete_Bucket():
    '''this function delete the bucket'''
    response=s3.delete_bucket(Bucket=target_bucket_name)

def main():
  Create_Bucket()

if  __name__ == "__main__":
    main()
