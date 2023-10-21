package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type startCh struct {
	startObjCh chan *s3.CopyObjectInput
}

type s3AccountInfo struct {
	srcProfile string
	srcBucket  string
	dstBucket  string
	srcRegion  string
	dstRegion  string
	s3ObjCh    startCh
}

func newS3AccountInfo() *s3AccountInfo {
	srcProfile := flag.String("srcProfile", "default", "set the source account profile")
	srcBucket := flag.String("srcBucket", "srcBucket", "set the source bucket name")
	dstBucket := flag.String("dstBucket", "dstBucket", "set the destination bucket name")
	srcRegion := flag.String("srcRegion", "us-east-1", "set the source region name -> default is us-east-1")
	dstRegion := flag.String("dstRegion", "us-east-1", "set the destination region name -> default is us-east-1")
	flag.Parse()
	return &s3AccountInfo{
		srcProfile: *srcProfile,
		srcBucket:  *srcBucket,
		dstBucket:  *dstBucket,
		srcRegion:  *srcRegion,
		dstRegion:  *dstRegion,
		s3ObjCh: startCh{
			startObjCh: make(chan *s3.CopyObjectInput),
		},
	}
}

func startConfig(profile, region string) (aws.Config, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(profile), config.WithDefaultRegion(region))
	if err != nil {
		return aws.Config{}, err
	}
	return cfg, nil
}

func (s *s3AccountInfo) sendS3Objects(s3Client s3.Client, listObjs *s3.ListObjectsV2Output) error {
outerfor:
	for {
		for _, listObj := range listObjs.Contents {
			input := &s3.CopyObjectInput{
				Bucket:     aws.String(s.dstBucket),
				CopySource: aws.String(s.srcBucket + "/" + *listObj.Key),
				Key:        aws.String(*listObj.Key),
			}
			s.s3ObjCh.startObjCh <- input
		}

		if listObjs.IsTruncated {
			fmt.Println("truncated true - new objects available")
			var err error
			listObjs, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket:            aws.String(s.srcBucket),
				ContinuationToken: listObjs.NextContinuationToken,
			})
			if err != nil {
				return err
			}
			continue
		}
		break outerfor
	}
	return nil
}

func (s *s3AccountInfo) S3CopyWorker(s3Client s3.Client) error {
	for {
		obj, ok := <-s.s3ObjCh.startObjCh
		if !ok {
			return nil
		}

		_, err := s3Client.CopyObject(context.TODO(), obj)
		if err != nil {
			return err
		}
		fmt.Println("copying:", *obj.Key)
	}
}

func main() {

	var (
		s          = newS3AccountInfo()
		t          = time.Now()
		wg         = sync.WaitGroup{}
		numWorkers = 20
	)

	fmt.Printf("%+v\n", s)

	s3Config, err := startConfig(s.srcProfile, s.srcRegion)
	if err != nil {
		fmt.Printf("error while initializing config: %v\n", err)
	}

	s3Client := s3.NewFromConfig(s3Config)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.S3CopyWorker(*s3Client)
		}()
	}

	listObjs, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s.srcBucket),
	})

	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	go func() {
		err = s.sendS3Objects(*s3Client, listObjs)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		close(s.s3ObjCh.startObjCh)
	}()

	wg.Wait()

	fmt.Printf("the whole process took: %.2f minutes\n", time.Since(t).Minutes())

}
