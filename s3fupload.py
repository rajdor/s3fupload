import json
import os
import hashlib
import time
import datetime
import boto3
import botocore
import sys
import logging
import os
from boto3.s3.transfer import S3Transfer

class MyFile:
    s3Resource = boto3.resource('s3')

    def __init__(self, localFile, bucket, path, suffix, tags):

        if not os.path.isfile(localFile):
            logger.error("Localfile is not a file or does not exist : " + str(localFile))
            exit(8)

        self.start_epoch = time.time()
        self.start = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.start_epoch))

        remoteFilename = os.path.basename(localFile)
        remoteFile = path + remoteFilename

        if suffix == '%%timestamp%%':
            remoteFile = remoteFile + "__" + time.strftime("%Y%m%d_%H%M%S", time.localtime(self.start_epoch))

        # clean up slashes just in case...
        remoteFile = remoteFile.replace("//", "/")

        self.name = localFile
        self.end_epoch = None
        self.end = None
        self.elapse = None

        self.localFile = {}
        self.localFile['filename'] = localFile
        self.localFile['size'] = os.path.getsize(self.localFile['filename'])
        self.localFile['md5'] = {}
        self.localFile['md5']['s3md5'] = ""
        self.localFile['md5']['start'] = ""
        self.localFile['md5']['end'] = ""
        self.localFile['md5']['elapse'] = ""
        self.localFile['modified'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(os.path.getmtime(self.localFile['filename'])))
        self.localFile['created'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(os.path.getctime(self.localFile['filename'])))

        try:
             MyFile.s3Resource.meta.client.head_bucket(Bucket=bucket)
        except botocore.client.ClientError:
             logger.error("The bucket does not exist or you have no access. : " + str(bucket))
             exit(8)

        self.remoteFile = {}
        self.remoteFile['bucket'] = bucket
        self.remoteFile['filename'] = remoteFile
        self.remoteFile['size'] = None
        self.remoteFile['last_modified'] = None
        self.remoteFile['s3md5'] = None
        self.remoteFile['last_modified_epoch'] = None
        self.remoteFile['last_modified'] = None
        self.remoteFile['tags'] = tags

        self.remoteFile['extra_args'] = {}
        self.remoteFile['extra_args']['ServerSideEncryption'] = "AES256"
        self.remoteFile['extra_args']['Metadata'] = {}
        self.remoteFile['extra_args']['Metadata']['local_file'] = localFile
        self.remoteFile['extra_args']['Metadata']['upload_batch'] = str(self.start_epoch)

        self.remoteFile['Tagging'] = tags

        self.remoteFile['upload'] = {}
        self.remoteFile['upload']['start'] = None
        self.remoteFile['upload']['end'] = None
        self.remoteFile['upload']['start_epoch'] = None
        self.remoteFile['upload']['end_epoch'] = None
        self.remoteFile['upload']['elapse'] = None
        self.remoteFile['upload']['consistency_check_retries'] = 0
        self.remoteFile['upload']['consistency_check_wait'] = 0
        self.remoteFile['upload']['consistency_check_wait'] = 0
        self.remoteFile['upload']['msg'] = None

        self.remoteFile['before'] = {}
        self.remoteFile['before']['exists'] = None
        self.remoteFile['before']['s3md5'] = None
        self.remoteFile['before']['size'] = None
        self.remoteFile['before']['last_modified_epoch'] = None
        self.remoteFile['before']['last_modified'] = None

    def dumpVars(self):
        return vars(self)

    def getlocalMD5(self):
        (self.localFile['md5']['s3md5'], self.localFile['md5']['start'], self.localFile['md5']['end'],
         self.localFile['md5']['elapse']) \
            = s3md5(self.localFile['filename'])
        self.localFile['md5']['start'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.localFile['md5']['start']))
        self.localFile['md5']['end'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.localFile['md5']['end']))
        return self.localFile['md5']['s3md5']

    def getBeforeRemoteFileProperties(self):
        try:
            objectSummary = MyFile.s3Resource.ObjectSummary(self.remoteFile['bucket'], self.remoteFile['filename'])
            self.remoteFile['before']['exists'] = True
            self.remoteFile['before']['s3md5'] = objectSummary.e_tag
            self.remoteFile['before']['s3md5'] = self.remoteFile['before']['s3md5'].replace("\"", "")
            self.remoteFile['before']['size'] = objectSummary.size

            last_modified = utcToLocal(objectSummary.last_modified)
            self.remoteFile['before']['last_modified_epoch'] = time.mktime(time.strptime(last_modified, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['before']['last_modified'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.remoteFile['before']['last_modified_epoch']))
            
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                self.remoteFile['before']['exists'] = False
            else:
                raise

    def getRemoteFileProperties(self):
        try:
            objectSummary = MyFile.s3Resource.ObjectSummary(self.remoteFile['bucket'], self.remoteFile['filename'])
            self.remoteFile['s3md5'] = objectSummary.e_tag
            self.remoteFile['s3md5'] = self.remoteFile['s3md5'].replace("\"", "")
            self.remoteFile['size'] = objectSummary.size
            

            last_modified = utcToLocal(objectSummary.last_modified)
            self.remoteFile['last_modified_epoch'] = time.mktime(time.strptime(last_modified, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['last_modified'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.remoteFile['last_modified_epoch']))
            
        except:
            raise

    def uploadFile(self):
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        self.remoteFile['upload']['start_epoch'] = time.time()
        self.remoteFile['upload']['start'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.remoteFile['upload']['start_epoch']))

        logger.debug("uploadFile : starting transfer : " + self.localFile['filename'])
        transfer.upload_file(self.localFile['filename'], self.remoteFile['bucket'], self.remoteFile['filename'], extra_args=self.remoteFile['extra_args'] )
        logger.debug("uploadFile : completed transfer : "  + self.localFile['filename'])
        logger.debug("adding tags : completed transfer : "  + self.localFile['filename'])

        if len(self.remoteFile['Tagging']) > 0:
            try:
                response = s3.put_object_tagging(
                                 Bucket=self.remoteFile['bucket'],
                                 Key=self.remoteFile['filename'],
                                 Tagging={ 'TagSet': self.remoteFile['Tagging'] })
            except:
                raise

        self.remoteFile['upload']['end_epoch'] = time.time()
        self.remoteFile['upload']['end'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.remoteFile['upload']['end_epoch']))
        self.remoteFile['upload']['elapse'] = self.remoteFile['upload']['end_epoch'] - self.remoteFile['upload']['start_epoch']

    def getUploadSuccess(self, retries, retryTime):
        logger.debug("Checking for upload success : "  + self.localFile['filename'] + " : " + str(retries))
        self.getRemoteFileProperties()

        self.remoteFile['upload']['consistency_check_retries'] = retries
        self.remoteFile['upload']['consistency_check_wait'] = self.remoteFile['upload']['consistency_check_wait'] + retryTime

        if self.localFile['md5']['s3md5'] != self.remoteFile['s3md5']:
            self.remoteFile['upload']['msg'] = "MD5 check failed"
            return False, self.remoteFile['upload']['msg']

        if self.localFile['size'] != self.remoteFile['size']:
            self.remoteFile['upload']['msg'] = "File size check failed"
            return False, self.remoteFile['upload']['msg']

        if not (self.remoteFile['upload']['start_epoch'] < self.remoteFile['last_modified_epoch'] < self.remoteFile['upload']['end_epoch']):
            self.remoteFile['upload']['msg'] = "Remote timestamp can't be before start of upload or after end of upload: " \
                                               + " : Last modified:"  + str(self.remoteFile['last_modified_epoch'])        \
                                               + " : Upload Started:" + str(self.remoteFile['upload']['start_epoch'])      \
                                               + " : Upload Ended:"   + str(self.remoteFile['upload']['end_epoch'])
            return False, self.remoteFile['upload']['msg']

        self.remoteFile['upload']['msg'] = "Passed all tests"
        logger.debug("Checking for upload success : "  + self.localFile['filename'] + " : Passed all tests")

        self.end_epoch = time.time()
        self.end = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.end_epoch))
        self.elapse = self.end_epoch - self.start_epoch

        return True, self.remoteFile['upload']['msg']


def get_logger(logger_name,create_file=False):

        class MyFormatter(logging.Formatter):
            """ Custom logger format to get nice timestamp"""
            converter = datetime.datetime.fromtimestamp
            def formatTime(self, record, datefmt=None):
                ct = self.converter(record.created)
                if datefmt:
                    s = ct.strftime(datefmt)
                else:
                    t = ct.strftime("%Y-%m-%d %H:%M:%S")
                    s = "%s,%03d" % (t, record.msecs)
                return s

        log = logging.getLogger(logger_name)
        log.setLevel(level=logging.INFO)

        formatter = MyFormatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')

        if create_file:
                fn = os.path.splitext(os.path.basename(__file__))[0] + ".log"
                fh = logging.FileHandler(fn)
                fh.setLevel(level=logging.DEBUG)
                fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setLevel(level=logging.DEBUG)
        ch.setFormatter(formatter)

        if create_file:
            log.addHandler(fh)

        log.addHandler(ch)
        return  log 

def utcToLocal(datestr):
    datestr = str(datestr)
    if str(datestr)[-6:] != "+00:00":
        logger.error("Unexpected date format in utcToLocal : " + str(datestr))
        exit(8)
    # change this 2020-10-03 23:47:19+00:00
    # into this 2020-10-03 23:47:19+0000
    datestr = datestr.replace("+00:00","+0000")
    UTC_datetime = datetime.datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S%z')
    UTC_datetime_timestamp = float(UTC_datetime.strftime("%s"))
    local_datetime_converted = datetime.datetime.fromtimestamp(UTC_datetime_timestamp)
    return (str(local_datetime_converted))

logger = get_logger("logger", False)

def s3md5(fname):
    #generate the AWS S3 Etag value on given (local file)
    # Max size in bytes before uploading in parts.
    multipartThreshold = 8 * 1024 * 1024
    # Size of parts when uploading in parts
    uploadPartSize = 8 * 1024 * 1024

    start = time.time()
    filesize = os.path.getsize(fname)
    hashObject = hashlib.md5()

    if filesize > multipartThreshold:
        md5s = []
        with open(fname, 'rb') as f:
           for block in iter(lambda: f.read(uploadPartSize), b''):
               md5s.append(hashlib.md5(block).digest())
        h = hashlib.md5(b''.join(md5s)).hexdigest() + '-' + str(len(md5s))

    else:
        with open(fname, "rb") as f:
            for block in iter(lambda: f.read(uploadPartSize), b''):
                hashObject.update(block)
        h = hashObject.hexdigest()

    end = time.time()
    elapse = end - start
    return h, start, end, elapse


def uploadFile(localFile, bucket, path, suffix, tags):
    f1 = MyFile(localFile, bucket, path, suffix, tags)
    logger.debug("uploadFile: " + str(localFile) + " : " + bucket + " : " + path + " : " + suffix)
    f1.getlocalMD5()
    f1.getBeforeRemoteFileProperties()

    f1.uploadFile()

    # Check that upload was successful, doesn't guarantee consistency unless you are writing a new file
    # worst case wait 7 minutes over 20 retries
    success = False
    retries = 0
    while success is False and retries <= 20:
        sleepAmt = retries * 2
        time.sleep(sleepAmt)
        success, msg = f1.getUploadSuccess(retries, sleepAmt)
        retries += 1
    return f1.dumpVars()


def main():
    logger.debug("Boto3 requires credentials, this script assumes you have a working '.aws' configuration. https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html")
    try:
        inputFile = sys.argv[1]
    except UnboundLocalError:
        logger.error("Missing parameter file")
        exit(8)
    except IndexError:
        logger.error("Missing parameter file")
        exit(8)

    logger.debug("Input file: " + inputFile)
    data = json.load(open(inputFile))

    start_epoch = time.time()
    batch = {}
    batch['batch']       = str(start_epoch)
    batch['start_epoch'] = start_epoch
    batch['start']       = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(batch['start_epoch']))
    batch['files'] = []
    batch['batch_counter'] = 0
    batch['batch_success'] = 0
    batch['batch_failed'] = 0

    for j in data:
        batch['batch_counter'] += 1
        logger.debug("Transfers : " + str(batch['batch_counter']) +"/" + str(len(data)) )
        
        localFile = j["localFile"]
        bucket    = j["bucket"]
        path      = j["remotePath"]
        if "remoteFileSuffix" in j:
            suffix = j["remoteFileSuffix"]
        else:
            suffix = None
        
        if "tags" in j:
            tags = j["tags"]
        else:
            tags = []

        temp = uploadFile(localFile, bucket, path, suffix, tags)
        if temp['remoteFile']['upload']['msg'] == "Passed all tests":
           batch['batch_success'] += 1
        else:
           batch['batch_failed'] += 1

        batch['files'].append(temp)

    batch['end_epoch'] = time.time()
    batch['end']       = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(batch['end_epoch']))
    batch['elapse']    = batch['end_epoch'] - batch['start_epoch']

    print (json.dumps(batch, indent=4, sort_keys=False))

if __name__ == "__main__":
    main()
