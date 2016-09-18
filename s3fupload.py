import json
import os
import hashlib
import binascii
import time
import datetime
import boto3
import botocore
import sys
from boto3.s3.transfer import S3Transfer

class MyFile:
    s3Resource = boto3.resource('s3')

    def __init__(self, localFile, bucket, path, suffix):

        if not os.path.isfile(localFile):
            print "Specified file does not exists: " + localFile
            exit(1)

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
        self.localFile['modified'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                   time.localtime(os.path.getmtime(self.localFile['filename'])))
        self.localFile['created'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                  time.localtime(os.path.getctime(self.localFile['filename'])))

        self.remoteFile = {}
        self.remoteFile['bucket'] = bucket
        self.remoteFile['filename'] = remoteFile
        self.remoteFile['size'] = None
        self.remoteFile['last_modified'] = None
        self.remoteFile['s3md5'] = None
        self.remoteFile['last_modified_epoch'] = None
        self.remoteFile['last_modified'] = None

        self.remoteFile['extra_args'] = {}
        self.remoteFile['extra_args']['ServerSideEncryption'] = "AES256"
        self.remoteFile['extra_args']['Metadata'] = {}
        self.remoteFile['extra_args']['Metadata']['local_file'] = localFile
        self.remoteFile['extra_args']['Metadata']['upload_batch'] = str(self.start_epoch)

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
        self.localFile['md5']['start'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                       time.localtime(self.localFile['md5']['start']))
        self.localFile['md5']['end'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.localFile['md5']['end']))
        return self.localFile['md5']['s3md5']

    def getBeforeRemoteFileProperties(self):
        try:
            objectSummary = MyFile.s3Resource.ObjectSummary(self.remoteFile['bucket'], self.remoteFile['filename'])
            self.remoteFile['before']['exists'] = True
            self.remoteFile['before']['s3md5'] = objectSummary.e_tag
            self.remoteFile['before']['s3md5'] = self.remoteFile['before']['s3md5'].replace("\"", "")
            self.remoteFile['before']['size'] = objectSummary.size
            btimeStr = str(utcToBrisbane(objectSummary.last_modified, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['before']['last_modified_epoch'] = time.mktime(time.strptime(btimeStr, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['before']['last_modified'] = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(
                self.remoteFile['before']['last_modified_epoch']))
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
            btimeStr = str(utcToBrisbane(objectSummary.last_modified, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['last_modified_epoch'] = time.mktime(time.strptime(btimeStr, '%Y-%m-%d %H:%M:%S'))
            self.remoteFile['last_modified'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                             time.localtime(self.remoteFile['last_modified_epoch']))
        except:
            raise

    def uploadFile(self):
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        self.remoteFile['upload']['start_epoch'] = time.time()
        self.remoteFile['upload']['start'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                           time.localtime(self.remoteFile['upload']['start_epoch']))

        transfer.upload_file(self.localFile['filename'], self.remoteFile['bucket'], self.remoteFile['filename'],
                             extra_args=self.remoteFile['extra_args']
                             )
        self.remoteFile['upload']['end_epoch'] = time.time()
        self.remoteFile['upload']['end'] = time.strftime("%Y/%m/%d-%H:%M:%S",
                                                         time.localtime(self.remoteFile['upload']['end_epoch']))
        self.remoteFile['upload']['elapse'] = self.remoteFile['upload']['end_epoch'] - self.remoteFile['upload'][
            'start_epoch']

    #  def setTag(self, k, v):
    #      # apparently u can't set tags but instead need to copy the entire file...
    #      https://github.com/boto/boto3/issues/389
    #
    #      k = "x-amz-meta-" + str(k)
    #      v = str(v)
    #      object  = MyFile.s3Resource.Object(self.remoteFile['bucket'],self.remoteFile['filename'])
    #      object.put(Metadata={k: v})

    def getUploadSuccess(self, retries, retryTime):

        # replace this with wait_until_exists??

        self.getRemoteFileProperties()

        self.remoteFile['upload']['consistency_check_retries'] = retries
        self.remoteFile['upload']['consistency_check_wait'] = self.remoteFile['upload'][
                                                                  'consistency_check_wait'] + retryTime

        if self.localFile['md5']['s3md5'] != self.remoteFile['s3md5']:
            self.remoteFile['upload']['msg'] = "MD5 check failed"
            return False, self.remoteFile['upload']['msg']

        if self.localFile['size'] != self.remoteFile['size']:
            self.remoteFile['upload']['msg'] = "File size check failed"
            return False, self.remoteFile['upload']['msg']

        if not (self.remoteFile['upload']['start_epoch'] < self.remoteFile['last_modified_epoch'] <
                        self.remoteFile['upload']['start_epoch'] + 10):
            self.remoteFile['upload']['msg'] = "Remote timestamp hasn't been updated : " + str(
                self.remoteFile['last_modified']) \
                                               + " : Upload Started:" + str(self.remoteFile['upload']['start_epoch']) \
                                               + " : Upload Ended:" + str(self.remoteFile['upload']['end_epoch'])
            return False, self.remoteFile['upload']['msg']

        self.remoteFile['upload']['msg'] = "Passed all tests"

        self.end_epoch = time.time()
        self.end = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(self.end_epoch))
        self.elapse = self.end_epoch - self.start_epoch

        # self.setTag("Status","OK")

        return True, self.remoteFile['upload']['msg']


def utcToBrisbane(dateString, date_format):
    # OMG date handling....

    dt_withouttz = str(dateString)[:-6]
    date_object = datetime.datetime.strptime(dt_withouttz, date_format)

    mytz_Brisbane = '+10:00'
    mytz_modifier = mytz_Brisbane[0:1]
    mytz_hours = mytz_Brisbane[1:3]
    mytz_minutes = mytz_Brisbane[4:6]
    mytz_seconds = int(mytz_minutes) * 60 + int(mytz_hours) * 3600
    if mytz_modifier == '+':
        date_object = date_object + datetime.timedelta(seconds=mytz_seconds)
    if mytz_modifier == '-':
        date_object = date_object - datetime.timedelta(seconds=mytz_seconds)

    return date_object


def s3md5(fname):
    # generate the AWS S3 Etag value on given (local file)

    # Max size in bytes before uploading in parts.
    multipartThreshold = 8 * 1024 * 1024

    # Size of parts when uploading in parts
    uploadPartSize = 8 * 1024 * 1024

    start = time.time()
    filesize = os.path.getsize(fname)
    hashObject = hashlib.md5()

    if filesize > multipartThreshold:
        partCount = 0
        md5String = ""
        with open(fname, "rb") as f:
            for block in iter(lambda: f.read(uploadPartSize), ""):
                hashObject = hashlib.md5()
                hashObject.update(block)
                md5String = md5String + binascii.unhexlify(hashObject.hexdigest())
                partCount += 1

                hashObject = hashlib.md5()
                hashObject.update(md5String)
        h = hashObject.hexdigest() + "-" + str(partCount)

    else:
        with open(fname, "rb") as f:
            for block in iter(lambda: f.read(uploadPartSize), ""):
                hashObject.update(block)
        h = hashObject.hexdigest()

    end = time.time()
    elapse = end - start
    return h, start, end, elapse


def uploadFile(localFile, bucket, path, suffix):
    f1 = MyFile(localFile, bucket, path, suffix)
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
    inputFile = sys.argv[1]

    # to do put these into the input file as options
    # date/time options for prefix & suffix
    # extra args for s3 upload, tags, encryption etc...
    # different remote filename

    # read input file in json format, loop through and upload files!
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
        
        localFile = j["localFile"]
        bucket    = j["bucket"]
        path      = j["remotePath"]
        if "remoteFileSuffix" in j:
            suffix = j["remoteFileSuffix"]
        else:
            suffix = None

        temp = uploadFile(localFile, bucket, path, suffix)
        if temp['remoteFile']['upload']['msg'] == "Passed all tests":
           batch['batch_success'] += 1    
        else:
           batch['batch_failed'] += 1    
        
        batch['files'].append(temp)

    batch['end_epoch'] = time.time()
    batch['end']       = time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(batch['end_epoch']))
    batch['elapse']    = batch['end_epoch'] - batch['start_epoch']
    
    print json.dumps(batch, indent=4, sort_keys=False)

        

if __name__ == "__main__":
    main()
