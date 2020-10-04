# s3fupload

s3fupload is for copying files from a local system to S3.
It's aim is to provide robust acknowledgement of s3 copy confirmation and timing.

   * Input is a JSON text file in the following form  
   * Currently, suffix is optional and only one option is available %%timestamp%%
   * For each file in the JSON input file
      * Get details of local file, including size, timestamp, md5 (AWS S3 E-Tag)
      * Check to see if existing remote file exists
      * Transfer file 
      * Get details of remote file, including size, timestamp, md5 (AWS S3 E-Tag)
      * Compare results
   * Output detailed results, time and other info as JSON to stdout

## 2020.10.04
   * fixed local vs utc time comparisons
   * added in S3 object tagging
   * removed test csv files and replaced with genfiles.sh to make test files

---

Example Command line:
```bash
python3 s3fupload.py ./example/single.json > ./example/single_example_outout.json
```

```bash
python3 s3fupload.py ./example/consignment.json > ./example/consignment_example_output.json
```
---

## Example single file

```json
[
 {
   "bucket": "jarrods-datalake",
   "remotePath": "landing/",
   "remoteFileSuffix": "%%timestamp%%",
   "localFile": "./example/file1MB.txt",
   "tags": [
             {"Key": "key1", "Value": "value1"}
           , {"Key": "key2", "Value": "value2"}
           ]
 }
]
```


Example output:
```json
{
    "batch": "1601777419.0340686",
    "start_epoch": 1601777419.0340686,
    "start": "2020/10/04-13:10:19",
    "files": [
        {
            "start_epoch": 1601777419.0345123,
            "start": "2020/10/04-13:10:19",
            "name": "./example/file1MB.txt",
            "end_epoch": 1601777426.2732933,
            "end": "2020/10/04-13:10:26",
            "elapse": 7.238780975341797,
            "localFile": {
                "filename": "./example/file1MB.txt",
                "size": 1048576,
                "md5": {
                    "s3md5": "415cb81369af0529313fa007b1a0d399",
                    "start": "2020/10/04-13:10:20",
                    "end": "2020/10/04-13:10:20",
                    "elapse": 0.013341903686523438
                },
                "modified": "2020/10/03-22:37:09",
                "created": "2020/10/03-22:37:09"
            },
            "remoteFile": {
                "bucket": "jarrods-datalake",
                "filename": "landing/file1MB.txt__20201004_131019",
                "size": 1048576,
                "last_modified": "2020/10/04-13:10:23",
                "s3md5": "415cb81369af0529313fa007b1a0d399",
                "last_modified_epoch": 1601777423.0,
                "tags": [
                    {
                        "Key": "key1",
                        "Value": "value1"
                    },
                    {
                        "Key": "key2",
                        "Value": "value2"
                    }
                ],
                "extra_args": {
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "local_file": "./example/file1MB.txt",
                        "upload_batch": "1601777419.0345123"
                    }
                },
                "Tagging": [
                    {
                        "Key": "key1",
                        "Value": "value1"
                    },
                    {
                        "Key": "key2",
                        "Value": "value2"
                    }
                ],
                "upload": {
                    "start": "2020/10/04-13:10:21",
                    "end": "2020/10/04-13:10:25",
                    "start_epoch": 1601777421.1702878,
                    "end_epoch": 1601777425.1662388,
                    "elapse": 3.995950937271118,
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
    "end_epoch": 1601777426.273302,
    "end": "2020/10/04-13:10:26",
    "elapse": 7.239233493804932
}

```
