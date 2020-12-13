# ----------------------------------------------------------------------------------
# Imports
# ----------------------------------------------------------------------------------
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import NoCredentialsError
import coloredlogs
import configparser
import logging
import os
import sys
import threading

# ----------------------------------------------------------------------------------
# Types
# ----------------------------------------------------------------------------------
class S3Api:
    def __init__(self):
        # Configuration
        config = configparser.ConfigParser()
        path_to_config = os.path.dirname(os.path.realpath(__file__))
        config.read(os.path.join(path_to_config, 'config.ini'))

        self.uploads = dict()
        parse_config = True
        index = 0
        while parse_config:
            bucket_key = 'bucket{}'.format(index)
            file_key = 'files{}'.format(index)

            if bucket_key in config['Uploads'] and file_key in config['Uploads']:
                self.uploads[config['Uploads'][bucket_key]] = config['Uploads'][file_key]
            else:
                parse_config = False

            index += 1

        # Grab keys
        self._access_key = config['AWS']['access']
        self._secret_key = config['AWS']['secret']

        # Initialize client
        self.s3 = boto3.client('s3',
                               aws_access_key_id=self._access_key,
                               aws_secret_access_key=self._secret_key)

        # Initialize resource
        self.s3_resource = boto3.resource('s3',
                                          aws_access_key_id=self._access_key,
                                          aws_secret_access_key=self._secret_key)

    # ------------------------------------------------------------------------------
    def upload_files(self):
        for bucket in self.uploads:
            files = [file.strip() for file in self.uploads[bucket].split(',')]
            for file in files:
                self._upload_file_to_s3(file, bucket)

    # ------------------------------------------------------------------------------
    def _upload_file_to_s3(self, local_file, bucket, s3_file=None):
        if not s3_file:
            s3_file = os.path.basename(local_file)

        if os.path.getsize(local_file) < 1073741824:  # File size is less than a gig
            try:
                self.s3.upload_file(local_file, bucket, s3_file)
                logger.info("\nUploaded {} to S3".format(local_file))
            except FileNotFoundError:
                logger.error("\nThe file {} was not found".format(local_file))
            except NoCredentialsError:
                logger.error("\nCredentials not available")
        else:
            try:
                self._multipart_upload(local_file, bucket, s3_file)
                logger.info("\nUploaded {} to S3".format(local_file))
            except FileNotFoundError:
                logger.error("\nThe file {} was not found".format(local_file))
            except NoCredentialsError:
                logger.error("\nCredentials not available")

    # ------------------------------------------------------------------------------
    def _multipart_upload(self, local_file, bucket, s3_file):
        transfer_config = TransferConfig(multipart_threshold=1024 * 25,
                                         multipart_chunksize=1024 * 25,
                                         use_threads=False)
        self.s3_resource.Object(bucket, s3_file).upload_file(local_file,
                                                             Config=transfer_config,
                                                             Callback=ProgressPercentage(local_file))

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

# ----------------------------------------------------------------------------------
# Globals
# ----------------------------------------------------------------------------------
# Initialize logging
coloredlogs.DEFAULT_LEVEL_STYLES['debug'] = {}
coloredlogs.DEFAULT_LEVEL_STYLES['info'] = {'color': 'green'}
logger = logging.getLogger(__name__)
coloredlogs.install(level='INFO')

# ----------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    api = S3Api()
    api.upload_files()
