# ----------------------------------------------------------------------------------
# Imports
# ----------------------------------------------------------------------------------
import boto3
from botocore.exceptions import NoCredentialsError
import coloredlogs
import configparser
import logging
import os

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

        try:
            self.s3.upload_file(local_file, bucket, s3_file)
            logger.info("Uploaded {} to S3".format(local_file))
        except FileNotFoundError:
            logger.error("The file {} was not found".format(local_file))
        except NoCredentialsError:
            logger.error("Credentials not available")

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
    try:
        api = S3Api()
        api.upload_files()
    except:
        logger.error('Failed to upload file to S3')
        pass
