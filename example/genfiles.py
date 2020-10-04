import codecs
import optparse
import lorem
import math
from tqdm import tqdm

PARSER = optparse.OptionParser()
PARSER.add_option('-o', '--output '      , action="store", dest="outFileName", help="Output File Name"         , default="")
PARSER.add_option('-s', '--size'         , action="store", dest="fileSize"   , help="Output file size: i.e. 1, 10, 100 etc... "        , default="")
PARSER.add_option('-u', '--unit'         , action="store", dest="sizeunit"   , help="Size unit: i.e. B, KB, MB, GB, TB etc... "        , default="")
OPTIONS, ARGS = PARSER.parse_args()

validUnits = ["B","KB","MB","GB","TB"]
if OPTIONS.sizeunit not in validUnits:
    print ("Error unit passed : " + str(OPTIONS.sizeunit) + " not a valid value " + str(validUnits))

multiplier = 1
if OPTIONS.sizeunit == "KB":
    multiplier = 1024
if OPTIONS.sizeunit == "MB":
    multiplier = 1048576
if OPTIONS.sizeunit == "GB":
    multiplier = 1073741824 
if OPTIONS.sizeunit == "TB":
    multiplier = 1099511627776

maxSize = int(OPTIONS.fileSize) * multiplier
size = 0

f = codecs.open(OPTIONS.outFileName, "w")

iterations = math.ceil(maxSize / 150)
pbar = tqdm(total=iterations)
while size < maxSize:
    tmpString=lorem.text()
    tmpStringLength=len(tmpString.encode('utf-8'))
    if tmpStringLength < 150: 
      tmpString = tmpString + "  " + lorem.text()
    tmpString.replace("\n", "").replace("\r", "").rstrip("\n")
    tmpString = tmpString[:150]
    tmpStringLength=len(tmpString.encode('utf-8'))

    if size + tmpStringLength > maxSize:
       tmpString = tmpString[0:maxSize-size]
    outrecord=tmpString
    f.write (outrecord)
    size = size + len(outrecord.encode('utf-8'))
    pbar.update(1)

f.close()
pbar.close()
