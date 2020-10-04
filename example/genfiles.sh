#!/bin/bash
python3 genfiles.py -o file1MB.txt -s 1 -u MB
python3 genfiles.py -o file10MB.txt -s 10 -u MB
python3 genfiles.py -o fileA10MB.txt -s 10 -u MB
python3 genfiles.py -o fileB10MB.txt -s 10 -u MB
python3 genfiles.py -o fileC10MB.txt -s 10 -u MB
python3 genfiles.py -o file100MB.txt -s 100 -u MB
python3 genfiles.py -o file1GB.txt -s 1 -u GB
python3 genfiles.py -o file10GB.txt -s 10 -u GB
#python3 genfiles.py -o file1TB.txt -s 1 -u TB
ls -alh
