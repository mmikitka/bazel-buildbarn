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
		return nil, err
	}
	return util.NewDigest(instance, &remoteexecution.Digest{
		Hash:      parts[0],
		SizeBytes: sizeBytes,
	})
}

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

type RedisServer struct {
	blobAccess BlobAccess
}

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
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return err
			}

			missing, err := rs.blobAccess.FindMissing(ctx, []*util.Digest{digest})
			if err != nil {
				return err
			}
			if len(missing) > 0 {
				conn.Write([]byte(":0\r\n"))
			} else {
				conn.Write([]byte(":1\r\n"))
			}
		} else if commandUpper == "GET" && parameters == 2 {
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return err
			}

			var b [4096]byte
			r := rs.blobAccess.Get(ctx, digest)
			if n, err := r.Read(b[:]); err == nil {
				if _, err := conn.Write([]byte(fmt.Sprintf("$%d\r\n", digest.GetSizeBytes()))); err != nil {
					return err
				}
				if _, err := conn.Write(b[:n]); err != nil {
					return err
				}
				if _, err := io.Copy(conn, r); err != nil {
					return err
				}
				if _, err := conn.Write([]byte("\r\n")); err != nil {
					return err
				}
			} else if s := status.Convert(err); s.Code() == codes.NotFound {
				conn.Write([]byte("$-1\r\n"))
			} else {
				return err
			}
		} else if commandUpper == "SET" && parameters == 3 {
			key, err := readBulkString(r)
			if err != nil {
				return err
			}
			digest, err := getDigestFromKey(key)
			if err != nil {
				return err
			}
			length, err := readBulkStringHeader(r)
			if err != nil {
				return err
			}

			l := io.LimitedReader{
				R: r,
				N: int64(length),
			}
			if err := rs.blobAccess.Put(ctx, digest, int64(length), ioutil.NopCloser(&l)); err != nil {
				return err
			}
			var buf [2]byte
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return err
			}
			if buf != [...]byte{'\r', '\n'} {
				return errors.New("String did not end with CR+NL")
			}
			conn.Write([]byte("+OK\r\n"))
		} else {
			return errors.New("Unknown command")
		}
	}
}

func (rs *RedisServer) HandleConnection(ctx context.Context, conn io.ReadWriteCloser) {
	if err := rs.handleCommands(ctx, conn); err != nil && err != io.EOF {
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
		log.Print(err)
	}
	conn.Close()
}
