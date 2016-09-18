# s3fupload

s3fupload is for copying files from a local system to S3.
Currently a hobby work in progress.  It's aim is to provide robust acknowledgement of s3 copy confirmation and timing.

Input is a JSON text file in the following form.  (suffix is optional)
```json
[
 {
   "bucket": "mydatalake",
   "remotePath": "landing/landsat/",
   "remoteFileSuffix": "%%timestamp%%",
   "localFile": "c:\\temp\\1\\hello.txt"
 },
 {
   "bucket": "mydatalake",
   "remotePath": "landing/landsat/",
   "remoteFileSuffix": "%%timestamp%%",
   "localFile": "c:\\temp\\1\\world.txt"
 }
]
```

Example Command line:
```bash
python myS3Loader.py loadthese.json  > output.json
```

Example output:
```json
{
    "files": [
        {
            "elapse": 4.042999982833862, 
            "end": "2016/09/18-13:08:33", 
            "name": "c:\\temp\\1\\hello.txt", 
            "remoteFile": {
                "s3md5": "b10a8db164e0754105b7a99be72e3fe5", 
                "bucket": "mydatalake", 
                "upload": {
                    "elapse": 1.3220000267028809, 
                    "end": "2016/09/18-13:08:32", 
                    "consistency_check_wait": 0, 
                    "start_epoch": 1474168110.839, 
                    "start": "2016/09/18-13:08:30", 
                    "end_epoch": 1474168112.161, 
                    "msg": "Passed all tests", 
                    "consistency_check_retries": 0
                }, 
                "filename": "landing/landsat/hello.txt__20160918_130829", 
                "extra_args": {
                    "Metadata": {
                        "local_file": "c:\\temp\\1\\hello.txt", 
                        "upload_batch": "1474168109.14"
                    }, 
                    "ServerSideEncryption": "AES256"
                }, 
                "last_modified": "2016/09/18-13:08:34", 
                "before": {
                    "last_modified_epoch": null, 
                    "size": null, 
                    "last_modified": null, 
                    "exists": false, 
                    "s3md5": null
                }, 
                "size": 11, 
                "last_modified_epoch": 1474168114.0
            }, 
            "start_epoch": 1474168109.145, 
            "start": "2016/09/18-13:08:29", 
            "end_epoch": 1474168113.188, 
            "localFile": {
                "size": 11, 
                "created": "2016/09/17-09:52:46", 
                "modified": "2016/09/17-14:34:55", 
                "md5": {
                    "start": "2016/09/18-13:08:29", 
                    "elapse": 0.003999948501586914, 
                    "end": "2016/09/18-13:08:29", 
                    "s3md5": "b10a8db164e0754105b7a99be72e3fe5"
                }, 
                "filename": "c:\\temp\\1\\hello.txt"
            }
        }, 
        {
            "elapse": 2.692000150680542, 
            "end": "2016/09/18-13:08:35", 
            "name": "c:\\temp\\1\\world.txt", 
            "remoteFile": {
                "s3md5": "4c24aac86aa49adce486631bf365098f", 
                "bucket": "mydatalake", 
                "upload": {
                    "elapse": 1.371999979019165, 
                    "end": "2016/09/18-13:08:34", 
                    "consistency_check_wait": 0, 
                    "start_epoch": 1474168113.457, 
                    "start": "2016/09/18-13:08:33", 
                    "end_epoch": 1474168114.829, 
                    "msg": "Passed all tests", 
                    "consistency_check_retries": 0
                }, 
                "filename": "landing/landsat/world.txt__20160918_130833", 
                "extra_args": {
                    "Metadata": {
                        "local_file": "c:\\temp\\1\\world.txt", 
                        "upload_batch": "1474168113.19"
                    }, 
                    "ServerSideEncryption": "AES256"
                }, 
                "last_modified": "2016/09/18-13:08:37", 
                "before": {
                    "last_modified_epoch": null, 
                    "size": null, 
                    "last_modified": null, 
                    "exists": false, 
                    "s3md5": null
                }, 
                "size": 13, 
                "last_modified_epoch": 1474168117.0
            }, 
            "start_epoch": 1474168113.188, 
            "start": "2016/09/18-13:08:33", 
            "end_epoch": 1474168115.88, 
            "localFile": {
                "size": 13, 
                "created": "2016/09/18-12:29:54", 
                "modified": "2016/09/18-12:30:12", 
                "md5": {
                    "start": "2016/09/18-13:08:33", 
                    "elapse": 0.004000186920166016, 
                    "end": "2016/09/18-13:08:33", 
                    "s3md5": "4c24aac86aa49adce486631bf365098f"
                }, 
                "filename": "c:\\temp\\1\\world.txt"
            }
        }
    ], 
    "elapse": 6.735000133514404, 
    "end": "2016/09/18-13:08:35", 
    "batch_counter": 2, 
    "batch": "1474168109.14", 
    "start_epoch": 1474168109.145, 
    "batch_failed": 0, 
    "start": "2016/09/18-13:08:29", 
    "end_epoch": 1474168115.88, 
    "batch_success": 2
}

```
