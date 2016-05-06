package ktail

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"io"
	"log"
	"reflect"
	"time"
)

type Record struct {
	*kinesis.Record
	Stream string
	Shard  string
}

type Follower struct {
	api kinesisiface.KinesisAPI
}

func NewFollowerWithAPI(api kinesisiface.KinesisAPI) *Follower {
	return &Follower{api}
}

func NewFollower(region string) *Follower {
	var configs []*aws.Config
	if region != "" {
		configs = append(configs, aws.NewConfig().WithRegion(region))
	}
	return NewFollowerWithAPI(kinesis.New(session.New(), configs...))
}

func (f *Follower) GetShards(stream string) ([]string, error) {
	var prevShard *string
	var shards []string
	for {
		params := &kinesis.DescribeStreamInput{
			StreamName:            &stream,
			ExclusiveStartShardId: prevShard,
		}
		if resp, err := f.api.DescribeStream(params); err != nil {
			return nil, err
		} else if len(resp.StreamDescription.Shards) == 0 {
			return shards, nil
		} else {
			for _, s := range resp.StreamDescription.Shards {
				shards = append(shards, *s.ShardId)
				prevShard = s.ShardId
			}
		}
	}
}

func (f *Follower) getShardIterator(stream, shard, iterType string) (string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:           &shard,
		ShardIteratorType: &iterType,
		StreamName:        &stream,
	}
	if resp, err := f.api.GetShardIterator(params); err != nil {
		return "", err
	} else {
		return *resp.ShardIterator, nil
	}
}

func (f *Follower) getRecords(iter string) (records []*kinesis.Record, nextIter *string, lag time.Duration, err error) {
	var resp *kinesis.GetRecordsOutput
	if resp, err = f.api.GetRecords(&kinesis.GetRecordsInput{ShardIterator: &iter}); err == nil {
		records, nextIter, lag = resp.Records, resp.NextShardIterator, time.Duration(*resp.MillisBehindLatest)*time.Millisecond
	}
	return
}

// ReaderResult indicates the outcome of shard reading procedure.
// Shard reading may end gracefully when a closed shard's last batch
// of records was read. In that case, Err will be set to io.EOF.
type ReaderResult struct {
	Stream, Shard string
	Err           error
}

// String causes ReaderResult to satisfy fmt.Stringer interface.
func (rr *ReaderResult) String() string { return fmt.Sprintf("%s(%s): %v", rr.Stream, rr.Shard, rr.Err) }

func (rr *ReaderResult) setError(err error) *ReaderResult {
	rr.Err = err
	return rr
}

func (f *Follower) ReadRecords(dst chan<- *Record, stream, shard, iterType string, rest time.Duration) <-chan *ReaderResult {
	rrc := make(chan *ReaderResult, 1)
	go func() {
		defer close(rrc)
		rr := &ReaderResult{stream, shard, io.EOF}
		startIter, err := f.getShardIterator(stream, shard, iterType)
		if err != nil {
			rrc <- rr.setError(err)
		} else {
			var records []*kinesis.Record
			var lag time.Duration
			const maxAttempts = 5
			attempt := 1
			for iter := &startIter; iter != nil; {
				time.Sleep(rest)
				attemptedIter := iter
				if records, iter, lag, err = f.getRecords(*attemptedIter); err != nil {
					if err, ok := err.(awserr.Error); ok && err.Code() == "ProvisionedThroughputExceededException" && attempt < maxAttempts {
						attempt, iter = attempt+1, attemptedIter
						log.Print(err.Message())
						log.Printf("Will make attempt %v in a bit...", attempt)
						continue
					}
					rrc <- rr.setError(err)
					return
				}
				attempt = 1
				for _, r := range records {
					dst <- &Record{r, stream, shard}
				}
				if lag > 5*time.Second {
					log.Printf("Reader for %s(%s) is %v behind", stream, shard, lag)
				}
			}
			rrc <- rr
		}
	}()
	return rrc
}

// merge, well, merges results from input channels (chans) onto the
// output channel (the one returned) until all of inputs are
// closed. It closes the output channel when done, too.
// Credit: remixed from https://godoc.org/github.com/eapache/channels#Multiplex
func merge(chans []<-chan *ReaderResult) <-chan *ReaderResult {
	out := make(chan *ReaderResult)
	go func() {
		defer close(out)
		n := len(chans)
		cases := make([]reflect.SelectCase, n)
		for i := range cases {
			cases[i].Dir, cases[i].Chan = reflect.SelectRecv, reflect.ValueOf(chans[i])
		}
		for n > 0 {
			chosen, recv, recvOK := reflect.Select(cases)
			if recvOK {
				out <- recv.Interface().(*ReaderResult)
			} else {
				cases[chosen].Chan = reflect.ValueOf(nil)
				n--
			}
		}
	}()
	return out
}

func (f *Follower) Tail(dst chan<- *Record, stream, iterType string, rest time.Duration) (<-chan *ReaderResult, error) {
	shards, err := f.GetShards(stream)
	if err != nil {
		return nil, err
	}
	results := make([]<-chan *ReaderResult, len(shards))
	for i, s := range shards {
		results[i] = f.ReadRecords(dst, stream, s, iterType, rest)
	}
	return merge(results), nil
}
