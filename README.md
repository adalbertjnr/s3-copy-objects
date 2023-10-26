# simple golang script to copy objects between buckets.

why use it instead of the CLI?

results of a test with 3000 objects in a s3 bucket:

CLI -> All 3000 objects were copied in 1 minute and 20 seconds.

GO -> All 3000 objects were copied in 16 seconds.

```
go run s3.go --srcProfile=default --srcBucket=bucket-a --dstBucket=bucket-b --srcRegion=us-east-1 --dstRegion=us-east-1
```
