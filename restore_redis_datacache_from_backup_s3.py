#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO LOOK FOR S3 BUCKET SPECIFIED IN THE MAIN FUNCTION AND IF FOUND, THEN DOWNLOAD ONTO LOCAL MACHINE  #
# WITH THE EXACT DIRECTORTY STRUCTURE TO BE UPLOADED ONTO REDIS                                                       #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       python restore_redis_datacache_from_backup_s3.py                                                              #
#                                                                                                                     #
#######################################################################################################################
import boto3
from pprint import pprint
import os


class AwsS3API(object):
    """
    This is to create the multiple aws instances on specified subnets
    """

    def __init__(self, s3_bucket_name):
        """
        To initialize the aws loadbalancer client to perform the operations on it.
        :param load_balancer_name:
        :param instance_id_list:
        """
        AWS_ACCESS_KEY_ID = '<>'
        AWS_SECRET_ACCESS_KEY = '<>'
        self.s3_client = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-west-2')
        self.s3_client_api = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-west-2')
        self.s3_bucket_name = s3_bucket_name

    def create_s3_bucket(self, acl, location_region):
        """
        To create new s3 bucket to store objects
        :return:
        """
        response = self.s3_client.create_bucket(ACL=acl, Bucket=self.s3_bucket_name, CreateBucketConfiguration={'LocationConstraint': location_region})
        pprint(response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(" ******** AWS S3 bucket %s has been created ***********" % self.s3_bucket_name)
        else:
            raise IOError("Unable to create the s3 bucket on aws")

    def check_if_s3_bucket_exists(self):
        """
        To ensure s3
        :return:
        """
        s3_ducket_exists = False
        response = self.s3_client_api.head_bucket(Bucket=self.s3_bucket_name)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("AWS S3 bucket already exists. It will use the existing bucket.")
            s3_ducket_exists = True
            return s3_ducket_exists
        else:
            print("AWS S3 bucket %s does not exists" % self.s3_bucket_name)
            return s3_ducket_exists

    def download_files_from_s3_to_ec2(self):
        """
        To copy the files from aws instance to s3 bucket
        :return:
        """
        if not self.check_if_s3_bucket_exists():
            print("AWS S3 bucket does not exists...Download cannot be occurred")

        print("************************** Starting s3 downloads *********************************")
        s3_bucket_list = self.s3_client_api.list_objects(Bucket=self.s3_bucket_name)['Contents']
        for s3_key in s3_bucket_list:
            s3_object = s3_key['Key']
            download_file_list = s3_object.split('/')
            download_file_name = download_file_list[1]

            if not s3_object.endswith("/"):
                print("In s3 object If case")
                self.s3_client_api.download_file(self.s3_bucket_name, s3_object, download_file_name)
                print("File %s has been downloaded from s3 bucket %s successfully" % (s3_object, self.s3_bucket_name))
            else:
                print("Directory structure has been created on the local EC2 instance to download")
                if not os.path.exists(s3_object):
                    os.makedirs(s3_object)


def main():
    """
    Main Program execution point
    :return:
    """
    # Initializing the bucket bucket
    s3_bucket_name = "my-prod-test"

    # Backup for the Redis AOF and RDB files
    redis_cache_files = AwsS3API(s3_bucket_name)
    redis_cache_files.download_files_from_s3_to_ec2()


# Main execution point
if __name__ == '__main__':
    main()
