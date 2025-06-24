package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof" // Register pprof handlers

	"github.com/valyala/fasthttp"
)

const (
	// Endpoint for the API
	apiURL = "https://tsserv.tinkermode.dev/data"
	// Entire process timeout.
	// Must complete entire process within this timeout.
	// Otherwise, print tentative result and exit.
	processTimeout = 5 * time.Minute
	// Request timeout
	requestTimeout = 100 * time.Second
)

func handleError(err error, callbackBeforeExit func()) {
	if err != nil {
		fmt.Println("Error:", err)
		if callbackBeforeExit != nil {
			callbackBeforeExit()
		}
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
	defer cancel()

	// validate command args, then obtain start and end time
	st, ed, isDebug, err := validateCommandArgs(os.Args[1:])
	handleError(err, nil)

	if isDebug {
		// print the start and end time
		fmt.Printf("Start time: %s, End time: %s\n", st.Format(time.RFC3339), ed.Format(time.RFC3339))

		// live profiling
		go func() {
			err = http.ListenAndServe("localhost:6060", nil)
			handleError(err, nil)
		}()
	}

	// fetch data
	stream, bodyStreamResp, err := fetch(st, ed, isDebug)
	if bodyStreamResp != nil {
		defer fasthttp.ReleaseResponse(bodyStreamResp)
	}
	handleError(err, nil)

	// tally up the data
	err = tally(ctx, stream)
	handleError(err, nil)

	if isDebug {
		takeMemProfile()
	}
}

func validateCommandArgs(args []string) (st time.Time, ed time.Time, isDebug bool, err error) {
	if len(args) < 2 {
		err = fmt.Errorf("invalid number of arguments. Usage: <start_time> <end_time>")
		return
	}

	if st, err = time.Parse(time.RFC3339, args[0]); err != nil {
		err = fmt.Errorf("invalid start time: %v, err: %w", args[0], err)
		return
	}

	if ed, err = time.Parse(time.RFC3339, args[1]); err != nil {
		err = fmt.Errorf("invalid end time: %v, err: %w", args[1], err)
		return
	}

	// make sure start time is before end time
	if st.After(ed) {
		err = fmt.Errorf("start time is after end time: %v, %v", st, ed)
		return
	}

	// The sec must be zero
	// Optional, but it's better to have it.
	// if st.Second() != 0 || ed.Second() != 0 {
	// 	err = fmt.Errorf("start time and end time must be at the beginning of the minute")
	// 	return
	// }

	// Check if debug mode is enabled
	if len(args) > 2 && args[2] == "debug" {
		isDebug = true
	}

	return
}

// TODO: Return resp if it's a body stream. I'm not sure what happen if immediate release the response.
func fetch(st, ed time.Time, isDebug bool) (stream io.Reader, resp *fasthttp.Response, err error) {
	var (
		url           = fmt.Sprintf("%s?begin=%s&end=%s", apiURL, st.Format(time.RFC3339), ed.Format(time.RFC3339))
		req           = fasthttp.AcquireRequest()
		hasBodyStream = false
	)
	req.SetRequestURI(url)
	req.Header.SetMethod("GET")

	resp = fasthttp.AcquireResponse()
	defer func() {
		if !hasBodyStream {
			// clean up response if it's not a body stream
			fasthttp.ReleaseResponse(resp)
		}
	}()

	err = fasthttp.DoTimeout(req, resp, requestTimeout)
	fasthttp.ReleaseRequest(req)
	if err != nil {
		err = fmt.Errorf("failed to fetch data: %w", err)
		return
	}

	if statusCode := resp.StatusCode(); statusCode != fasthttp.StatusOK {
		err = fmt.Errorf("unexpected status code: %d", statusCode)
		return
	}

	// make sure content type is text/plain
	if contentType := resp.Header.ContentType(); !bytes.HasPrefix(contentType, []byte("text/plain")) {
		err = fmt.Errorf("unexpected Content-Type: %s", contentType)
		return
	}

	// print content length in KB order
	if isDebug {
		fmt.Printf("Content-Length: %d KB\n", resp.Header.ContentLength()/1024)
	}

	if resp.IsBodyStream() {
		// from the doc, more than 10MB will be returned as a body stream
		// But, not works as the server doesn't support it
		// It's required server support: `Transfer-Encoding: chunked` or `Content-Length` is set
		hasBodyStream = true
		stream = resp.BodyStream()
		if isDebug {
			// haven't reach here yet
			fmt.Println("body stream enabled")
		}
	} else {
		data := resp.Body()
		stream = bytes.NewReader(data)
		if isDebug {
			// print the size of the data by KB order
			fmt.Printf("Data size: %d KB\n", len(data)/1024)
		}
	}

	return
}

func tally(ctx context.Context, stream io.Reader) (err error) {
	var (
		n             int
		writer        = bufio.NewWriter(os.Stdout)
		buf           = make([]byte, 30)
		prevTimeSlot  [13]byte
		score         float64
		sum           float64
		count         int
		tallyAndPrint = func(timeSlot [13]byte, sum float64, count int) {
			avg := sum / float64(count)
			writer.WriteString(fmt.Sprintf("%s:00:00Z %8.4f\n", timeSlot, avg))
		}
	)
	defer writer.Flush()

	for {
		// make sure timeout is not reached
		if ctx.Err() != nil {
			err = fmt.Errorf("timeout reached(%s seconds). please extend the timeout: %w", requestTimeout.String(), ctx.Err())
			return
		}

		// read from stream
		n, err = stream.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			err = fmt.Errorf("read error: %w", err)
			return
		}

		// if there is data to process
		if n > 0 {
			// We assume the data format is always correct.
			// YYYY-MM-DDTHH:MM:SSZ 000.0000\n
			// To confirm this, just check the last byte. make sure the last is new line
			if buf[n-1] != '\n' {
				err = fmt.Errorf("the last is not a new line. invalid data format: %s", buf[:n])
				return
			}

			// extract `YYYY-MM-DD HH` part
			timeSlot := buf[:13]
			// extract the number
			score, err = strconv.ParseFloat(strings.TrimSpace(string(buf[21:29])), 32)
			if err != nil {
				err = fmt.Errorf("parse error: %w", err)
				return
			}

			if count == 0 {
				// The fist iteration, set the prev time slot
				copy(prevTimeSlot[:], timeSlot)
			}

			if bytes.Equal(timeSlot, prevTimeSlot[:]) {
				// within the same time slot, go to next
				sum += score
				count++
				continue
			}

			// tally up the score
			tallyAndPrint(prevTimeSlot, sum, count)

			// Go to next time slot
			copy(prevTimeSlot[:], timeSlot)
			count = 1
			sum = score
		}
	}

	// tally up the last time slot
	tallyAndPrint(prevTimeSlot, sum, count)

	return nil
}

func takeMemProfile() {
	// Dump heap profile at end
	f, err := os.Create("mem.prof")
	handleError(err, nil)
	defer f.Close()

	// Force GC to get up-to-date statistics
	runtime.GC()

	err = pprof.WriteHeapProfile(f)
	handleError(err, nil)
}
