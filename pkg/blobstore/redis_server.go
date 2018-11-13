package blobstore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getDigestFromKey converts key strings provided by the client to
// digests. This function does the inverse of Digest.GetKey().
// TODO(edsch): Should this be in pkg/util/digest.go?
func getDigestFromKey(key string) (*util.Digest, error) {
	parts := strings.SplitN(key, "|", 3)
	instance := ""
	switch len(parts) {
	case 2:
	case 3:
		instance = parts[2]
	default:
		return nil, errors.New("Invalid key format")
	}
	sizeBytes, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse size in digest: %s", err)
	}
	return util.NewDigest(instance, &remoteexecution.Digest{
		Hash:      parts[0],
		SizeBytes: sizeBytes,
	})
}

// readArrayCount parses an array header ("*123\r\n") from user input.
func readArrayCount(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	if len(line) == 0 || line[0] != '*' {
		return 0, errors.New("Expected * at start of array")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(line[1:]), 10, 0)
	return int(n), err
}

// readArrayCount parses a string header ("$123\r\n") from user input.
func readBulkStringHeader(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	if len(line) == 0 || line[0] != '$' {
		return 0, errors.New("Expected $ at start of bulk string")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(line[1:]), 10, 0)
	return int(n), err
}

// readArrayCount parses a string header from user input, followed by
// fetching the actual string that follows.
func readBulkString(r *bufio.Reader) (string, error) {
	length, err := readBulkStringHeader(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if !bytes.HasSuffix(buf, []byte("\r\n")) {
		return "", errors.New("String did not end with CR+NL")
	}
	return string(buf[:length]), nil
}

// RedisServer implements a very simple Redis compatible server,
// granting access to a BlobAccess. It implements the following three
// commands:
//
// - EXISTS <key>
// - GET <key>
// - PUT <key> <value>
//
// Unlike a plain Redis server, keys have to match the format
// "hash|size" or "hash|size|instance".
//
// TODO(edsch): The GET operation currently assumes the object size is
// equal to the size in the digest, restricting the use of this server
// to the Content Addressable Storage.
type RedisServer struct {
	blobAccess BlobAccess
}

// NewRedisServer creates a RedisServer object.
func NewRedisServer(blobAccess BlobAccess) *RedisServer {
	return &RedisServer{
		blobAccess: blobAccess,
	}
}

func (rs *RedisServer) handleCommands(ctx context.Context, conn io.ReadWriter) error {
	r := bufio.NewReader(conn)
	for {
		parameters, err := readArrayCount(r)
		if err != nil {
			return err
		}
		commandName, err := readBulkString(r)
		if err != nil {
			return err
		}
		commandUpper := strings.ToUpper(commandName)
		if commandUpper == "EXISTS" && parameters == 2 {
			// EXISTS <key>.
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return fmt.Errorf("Failed to convert key to digest: %s", err)
			}

			missing, err := rs.blobAccess.FindMissing(ctx, []*util.Digest{digest})
			if err != nil {
				return fmt.Errorf("Failed to find missing blobs: %s", err)
			}
			response := []byte(":1\r\n")
			if len(missing) > 0 {
				// Key does not exist.
				response = []byte(":0\r\n")
			}
			if _, err := conn.Write(response); err != nil {
				return fmt.Errorf("Failed to write response to client: %s", err)
			}
		} else if commandUpper == "GET" && parameters == 2 {
			// GET <key>.
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return fmt.Errorf("Failed to convert key to digest: %s", err)
			}

			// Read the first chunk of data to determine whether a
			// success or error response needs to be returned.
			var b [4096]byte
			r := rs.blobAccess.Get(ctx, digest)
			if n, err := r.Read(b[:]); err == nil || err == io.EOF {
				if _, err := conn.Write([]byte(fmt.Sprintf("$%d\r\n", digest.GetSizeBytes()))); err != nil {
					return fmt.Errorf("Failed to write response size to client: %s", err)
				}
				if _, err := conn.Write(b[:n]); err != nil {
					return fmt.Errorf("Failed to write response data to client: %s", err)
				}
				if _, err := io.Copy(conn, r); err != nil {
					return fmt.Errorf("Failed to write response data to client: %s", err)
				}
				if _, err := conn.Write([]byte("\r\n")); err != nil {
					return fmt.Errorf("Failed to write trailing CR+LF: %s", err)
				}
			} else if s := status.Convert(err); s.Code() == codes.NotFound {
				// Key does not exist.
				if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
					return fmt.Errorf("Failed to write response to client: %s", err)
				}
			} else {
				return fmt.Errorf("Failed to get blob: %s", err)
			}
		} else if commandUpper == "SET" && parameters == 3 {
			// SET <key> <value>.
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return fmt.Errorf("Failed to convert key to digest: %s", err)
			}
			length, err := readBulkStringHeader(r)
			if err != nil {
				return fmt.Errorf("Failed to read bulk string header from client: %s", err)
			}

			l := io.LimitedReader{
				R: r,
				N: int64(length),
			}
			if err := rs.blobAccess.Put(ctx, digest, int64(length), ioutil.NopCloser(&l)); err != nil {
				return fmt.Errorf("Failed to put blob: %s", err)
			}
			var buf [2]byte
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return fmt.Errorf("Failed to read CR+LF from client: %s", err)
			}
			if buf != [...]byte{'\r', '\n'} {
				return errors.New("CR+LF at end of value missing")
			}
			conn.Write([]byte("+OK\r\n"))
		} else {
			return fmt.Errorf("Unknown command: %s", commandName)
		}
	}
}

// HandleConnection processes commands received on a network connection,
// translating them to calls on the underlying BlobAccess.
func (rs *RedisServer) HandleConnection(ctx context.Context, conn io.ReadWriteCloser) {
	if err := rs.handleCommands(ctx, conn); err != nil && err != io.EOF {
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
		log.Print(err)
	}
	conn.Close()
}
