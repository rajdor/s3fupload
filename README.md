# s3fupload

s3fupload is for copying files from a local system to S3.
It's aim is to provide robust acknowledgement of s3 copy confirmation and timing.

Input is a JSON text file in the following form.  
Currently, suffix is optional and only one option is available %%timestamp%%

Example Command line:
```bash
python3 s3fupload.py ./example/single.json > ./example/single_example_outout.json
```

```bash
python3 s3fupload.py ./example/consignment.json > ./example/consignment_example_output.json
```


## Example single file

```json
[
 {
   "bucket": "jarrods-datalake",
   "remotePath": "landing/",
   "remoteFileSuffix": "%%timestamp%%",
   "localFile": "./example/CLAIM_STATUS.csv"
 }
]
```


Example output:
```json
{
    "batch": "1587280908.1078215",
    "start_epoch": 1587280908.1078215,
    "start": "2020/04/19-17:21:48",
    "files": [
        {
            "start_epoch": 1587280908.107864,
            "start": "2020/04/19-17:21:48",
            "name": "./example/CLAIM_STATUS.csv",
            "end_epoch": 1587280910.6476126,
            "end": "2020/04/19-17:21:50",
            "elapse": 2.5397486686706543,
            "localFile": {
                "filename": "./example/CLAIM_STATUS.csv",
                "size": 95,
                "md5": {
                    "s3md5": "6751f56d89b0c3fe7923d9bb5af00853",
                    "start": "2020/04/19-17:21:48",
                    "end": "2020/04/19-17:21:48",
                    "elapse": 7.224082946777344e-05
                },
                "modified": "2020/04/19-15:15:48",
                "created": "2020/04/19-15:23:58"
            },
            "remoteFile": {
                "bucket": "jarrods-datalake",
                "filename": "landing/CLAIM_STATUS.csv__20200419_172148",
                "size": 95,
                "last_modified": "2020/04/19-17:21:51",
                "s3md5": "6751f56d89b0c3fe7923d9bb5af00853",
                "last_modified_epoch": 1587280911.0,
                "extra_args": {
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "local_file": "./example/CLAIM_STATUS.csv",
                        "upload_batch": "1587280908.107864"
                    }
                },
                "upload": {
                    "start": "2020/04/19-17:21:49",
                    "end": "2020/04/19-17:21:50",
                    "start_epoch": 1587280909.2390258,
                    "end_epoch": 1587280910.409092,
                    "elapse": 1.1700661182403564,
                    "consistency_check_retries": 0,
                    "consistency_check_wait": 0,
                    "msg": "Passed all tests"
                },
                "before": {
                    "exists": false,
                    "s3md5": null,
                    "size": null,
                    "last_modified_epoch": null,
                    "last_modified": null
                }
            }
        }
    ],
    "batch_counter": 1,
    "batch_success": 1,
    "batch_failed": 0,
    "end_epoch": 1587280910.6476202,
    "end": "2020/04/19-17:21:50",
    "elapse": 2.5397987365722656
}
```
