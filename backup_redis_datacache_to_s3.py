#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO LOOK FOR THE FILES SPECIFIED IN THE LIST AND IF FOUND, THEN UPLOADS THEM ONTO S3 BUCKET SPECIFIED #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       python backup_redis_datacache_to_s3.py                                                                        #
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

    def upload_files_from_ec2_to_s3(self, file_list_dict):
        """
        To copy the files from aws instance to s3 bucket
        :return:
        """
        if not self.check_if_s3_bucket_exists():
            print("*************** Creating new S3 bucket...*****************")
            self.create_s3_bucket('public-read-write', location_region='us_west-2')

        print("************************** Starting s3 uploads *********************************")
        for key, value in file_list_dict.items():
            for val in value:
                full_copy_file_path = os.path.join(key, val)
                try:
                    sub_dir = key[9:]
                    destination = os.path.join(sub_dir, val)
                    self.s3_client.meta.client.upload_file(full_copy_file_path, self.s3_bucket_name, destination)
                except IOError as e:
                    print("Error occurred while copying the file over s3", e)
                finally:
                    print("File %s has been uploaded onto s3 bucket %s successfully" % (full_copy_file_path, self.s3_bucket_name))


def main():
    """
    Main Program execution point
    :return:
    """
    # Initializing the bucket bucket
    s3_bucket_name = "my-prod-test"

    # Directory stucture and file lists
    redis_backup_dict = {'/var/lib/redis': ['appendonly.aof', 'dump.rdb']
                          }
    # Backup for the "AAALogs"
    aaa = AwsS3API(s3_bucket_name)
    aaa.upload_files_from_ec2_to_s3(redis_backup_dict)


# Main execution point
if __name__ == '__main__':
    main()
