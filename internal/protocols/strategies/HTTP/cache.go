package HTTP

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beelzebub-labs/beelzebub/v3/internal/parser"
)

const defaultHTTPCacheTTL = 24 * time.Hour

type httpPageCache interface {
	Get(ctx context.Context, key string) (httpResponse, bool, error)
	Set(ctx context.Context, key string, resp httpResponse) error
}

type redisPageCache struct {
	addr     string
	password string
	db       int
	ttl      time.Duration
	timeout  time.Duration
}

func newHTTPPageCache(conf parser.HTTPCache) httpPageCache {
	if !conf.Enabled {
		return nil
	}
	addr := strings.TrimSpace(conf.RedisAddr)
	if addr == "" {
		addr = "localhost:6379"
	}
	ttl := defaultHTTPCacheTTL
	if conf.TTLSeconds > 0 {
		ttl = time.Duration(conf.TTLSeconds) * time.Second
	}
	return &redisPageCache{
		addr:     addr,
		password: conf.RedisPass,
		db:       conf.RedisDB,
		ttl:      ttl,
		timeout:  2 * time.Second,
	}
}

func httpCacheKey(servConf parser.BeelzebubServiceConfiguration, command parser.Command, request *http.Request, body string) string {
	bodySum := sha256.Sum256([]byte(body))
	source := strings.Join([]string{
		servConf.Address,
		servConf.Description,
		servConf.ServerName,
		command.Name,
		command.Plugin,
		request.Method,
		request.Host,
		cacheRequestURI(request),
		hex.EncodeToString(bodySum[:]),
	}, "\x00")
	sum := sha256.Sum256([]byte(source))
	return "beelzebub:http-page:" + hex.EncodeToString(sum[:])
}

func cacheRequestURI(request *http.Request) string {
	if request.RequestURI != "" {
		return request.RequestURI
	}
	if request.URL != nil {
		return request.URL.RequestURI()
	}
	return ""
}

func (r *redisPageCache) Get(ctx context.Context, key string) (httpResponse, bool, error) {
	reply, err := r.do(ctx, "GET", key)
	if err != nil {
		return httpResponse{}, false, err
	}
	data, ok := reply.(string)
	if !ok {
		return httpResponse{}, false, nil
	}
	var resp httpResponse
	if err := json.Unmarshal([]byte(data), &resp); err != nil {
		return httpResponse{}, false, err
	}
	return resp, true, nil
}

func (r *redisPageCache) Set(ctx context.Context, key string, resp httpResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if r.ttl > 0 {
		_, err = r.do(ctx, "SET", key, string(data), "EX", strconv.Itoa(int(r.ttl.Seconds())))
		return err
	}
	_, err = r.do(ctx, "SET", key, string(data))
	return err
}

func (r *redisPageCache) do(ctx context.Context, args ...string) (any, error) {
	dialer := net.Dialer{Timeout: r.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(r.timeout))
	}

	reader := bufio.NewReader(conn)
	if r.password != "" {
		if err := writeRedisCommand(conn, "AUTH", r.password); err != nil {
			return nil, err
		}
		if _, err := readRedisReply(reader); err != nil {
			return nil, err
		}
	}
	if r.db > 0 {
		if err := writeRedisCommand(conn, "SELECT", strconv.Itoa(r.db)); err != nil {
			return nil, err
		}
		if _, err := readRedisReply(reader); err != nil {
			return nil, err
		}
	}
	if err := writeRedisCommand(conn, args...); err != nil {
		return nil, err
	}
	return readRedisReply(reader)
}

func writeRedisCommand(w io.Writer, args ...string) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func readRedisReply(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		line, err := readRedisLine(r)
		return line, err
	case '-':
		line, err := readRedisLine(r)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	case ':':
		line, err := readRedisLine(r)
		if err != nil {
			return nil, err
		}
		return strconv.ParseInt(line, 10, 64)
	case '$':
		line, err := readRedisLine(r)
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, nil
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return string(data[:size]), nil
	default:
		return nil, fmt.Errorf("unexpected redis reply prefix %q", prefix)
	}
}

func readRedisLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
