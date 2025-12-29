/*
 * Copyright 2025 coze-dev Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	cosv5 "github.com/tencentyun/cos-go-sdk-v5"

	"github.com/coze-dev/coze-studio/backend/infra/storage"
	"github.com/coze-dev/coze-studio/backend/infra/storage/impl/internal/fileutil"
	"github.com/coze-dev/coze-studio/backend/pkg/goutil"
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
	"github.com/coze-dev/coze-studio/backend/pkg/taskgroup"
)

type cosClient struct {
	client     *cosv5.Client
	bucketName string
	secretID   string
	secretKey  string
}

func New(ctx context.Context, secretID, secretKey, bucketName, endpoint, region string) (storage.Storage, error) {
	c, err := getCosClient(ctx, secretID, secretKey, bucketName, endpoint, region)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func getCosClient(ctx context.Context, secretID, secretKey, bucketName, endpoint, region string) (*cosClient, error) {
	baseURL, err := buildBaseURL(bucketName, endpoint, region)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Transport: &cosv5.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	}

	client := cosv5.NewClient(baseURL, httpClient)
	c := &cosClient{
		client:     client,
		bucketName: bucketName,
		secretID:   secretID,
		secretKey:  secretKey,
	}

	if err := c.CheckAndCreateBucket(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func buildBaseURL(bucketName, endpoint, region string) (*cosv5.BaseURL, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("cos bucket name is empty")
	}

	if endpoint == "" {
		if region == "" {
			return nil, fmt.Errorf("cos endpoint is empty and region is missing")
		}
		endpoint = fmt.Sprintf("https://cos.%s.myqcloud.com", region)
	}

	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}

	serviceURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse cos endpoint failed: %w", err)
	}

	serviceURL.Path = ""
	serviceURL.RawQuery = ""
	serviceURL.Fragment = ""

	bucketHost := serviceURL.Host
	if !strings.HasPrefix(bucketHost, bucketName+".") {
		bucketHost = bucketName + "." + bucketHost
	}

	bucketURL := *serviceURL
	bucketURL.Host = bucketHost

	return &cosv5.BaseURL{
		BucketURL:  &bucketURL,
		ServiceURL: serviceURL,
	}, nil
}

func (c *cosClient) CheckAndCreateBucket(ctx context.Context) error {
	exist, err := c.client.Bucket.IsExist(ctx)
	if err != nil {
		return fmt.Errorf("check bucket failed: %w", err)
	}

	if exist {
		return nil
	}

	_, err = c.client.Bucket.Put(ctx, &cosv5.BucketPutOptions{
		XCosACL: "private",
	})
	if err != nil {
		return fmt.Errorf("create bucket failed: %w", err)
	}

	return nil
}

func (c *cosClient) PutObject(ctx context.Context, objectKey string, content []byte, opts ...storage.PutOptFn) error {
	opts = append(opts, storage.WithObjectSize(int64(len(content))))
	return c.PutObjectWithReader(ctx, objectKey, bytes.NewReader(content), opts...)
}

func (c *cosClient) PutObjectWithReader(ctx context.Context, objectKey string, content io.Reader, opts ...storage.PutOptFn) error {
	option := storage.PutOption{}
	for _, opt := range opts {
		opt(&option)
	}

	putOption := &cosv5.ObjectPutOptions{
		ObjectPutHeaderOptions: &cosv5.ObjectPutHeaderOptions{},
	}

	if option.ContentType != nil {
		putOption.ContentType = *option.ContentType
	}
	if option.ContentEncoding != nil {
		putOption.ContentEncoding = *option.ContentEncoding
	}
	if option.ContentDisposition != nil {
		putOption.ContentDisposition = *option.ContentDisposition
	}
	if option.ContentLanguage != nil {
		putOption.ContentLanguage = *option.ContentLanguage
	}
	if option.Expires != nil {
		putOption.Expires = option.Expires.UTC().Format(http.TimeFormat)
	}
	if option.ObjectSize > 0 {
		putOption.ContentLength = option.ObjectSize
	}
	if len(option.Tagging) > 0 {
		if putOption.ObjectPutHeaderOptions.XOptionHeader == nil {
			putOption.ObjectPutHeaderOptions.XOptionHeader = &http.Header{}
		}
		putOption.ObjectPutHeaderOptions.XOptionHeader.Set("x-cos-tagging", goutil.MapToQuery(option.Tagging))
	}

	_, err := c.client.Object.Put(ctx, objectKey, content, putOption)
	if err != nil {
		return fmt.Errorf("put object failed: %w", err)
	}

	return nil
}

func (c *cosClient) GetObject(ctx context.Context, objectKey string) ([]byte, error) {
	resp, err := c.client.Object.Get(ctx, objectKey, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (c *cosClient) DeleteObject(ctx context.Context, objectKey string) error {
	_, err := c.client.Object.Delete(ctx, objectKey)
	return err
}

func (c *cosClient) GetObjectUrl(ctx context.Context, objectKey string, opts ...storage.GetOptFn) (string, error) {
	opt := storage.GetOption{}
	for _, optFn := range opts {
		optFn(&opt)
	}

	expire := int64(7 * 24 * 60 * 60)
	if opt.Expire > 0 {
		expire = opt.Expire
	}

	url, err := c.client.Object.GetPresignedURL(ctx, http.MethodGet, objectKey, c.secretID, c.secretKey, time.Duration(expire)*time.Second, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (c *cosClient) ListObjectsPaginated(ctx context.Context, input *storage.ListObjectsPaginatedInput, opts ...storage.GetOptFn) (*storage.ListObjectsPaginatedOutput, error) {
	if input == nil {
		return nil, fmt.Errorf("input cannot be nil")
	}
	if input.PageSize <= 0 {
		return nil, fmt.Errorf("page size must be positive")
	}

	result, _, err := c.client.Bucket.Get(ctx, &cosv5.BucketGetOptions{
		Prefix:  input.Prefix,
		Marker:  input.Cursor,
		MaxKeys: input.PageSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list objects failed: %w", err)
	}

	files := make([]*storage.FileInfo, 0, len(result.Contents))
	for _, obj := range result.Contents {
		if obj.Size == 0 && strings.HasSuffix(obj.Key, "/") {
			logs.CtxDebugf(ctx, "[COS][ListObjectsPaginated] skip dir: %s", obj.Key)
			continue
		}

		files = append(files, &storage.FileInfo{
			Key:          obj.Key,
			LastModified: parseCosTime(obj.LastModified),
			ETag:         strings.Trim(obj.ETag, "\""),
			Size:         obj.Size,
		})
	}

	opt := storage.GetOption{}
	for _, optFn := range opts {
		optFn(&opt)
	}

	if opt.WithTagging {
		taskGroup := taskgroup.NewTaskGroup(ctx, 5)
		for idx := range files {
			f := files[idx]
			taskGroup.Go(func() error {
				tagging, _, err := c.client.Object.GetTagging(ctx, f.Key)
				if err != nil {
					return err
				}

				f.Tagging = cosTagsToMap(tagging.TagSet)
				return nil
			})
		}

		if err := taskGroup.Wait(); err != nil {
			return nil, err
		}
	}

	if opt.WithURL {
		files, err = fileutil.AssembleFileUrl(ctx, &opt.Expire, files, c)
		if err != nil {
			return nil, err
		}
	}

	return &storage.ListObjectsPaginatedOutput{
		Files:       files,
		Cursor:      result.NextMarker,
		IsTruncated: result.IsTruncated,
	}, nil
}

func (c *cosClient) ListAllObjects(ctx context.Context, prefix string, opts ...storage.GetOptFn) ([]*storage.FileInfo, error) {
	const (
		defaultPageSize = 100
		maxListObjects  = 10000
	)

	files := make([]*storage.FileInfo, 0, defaultPageSize)
	cursor := ""

	for {
		output, err := c.ListObjectsPaginated(ctx, &storage.ListObjectsPaginatedInput{
			Prefix:   prefix,
			PageSize: defaultPageSize,
			Cursor:   cursor,
		}, opts...)
		if err != nil {
			return nil, err
		}

		files = append(files, output.Files...)

		if len(files) >= maxListObjects {
			logs.CtxErrorf(ctx, "[COS][ListAllObjects] max list objects reached, total: %d", len(files))
			break
		}

		if !output.IsTruncated || output.Cursor == "" {
			break
		}

		cursor = output.Cursor
	}

	return files, nil
}

func (c *cosClient) HeadObject(ctx context.Context, objectKey string, opts ...storage.GetOptFn) (*storage.FileInfo, error) {
	resp, err := c.client.Object.Head(ctx, objectKey, nil)
	if err != nil {
		if cosv5.IsNotFoundError(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, err
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	lastModified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))

	fileInfo := &storage.FileInfo{
		Key:          objectKey,
		LastModified: lastModified,
		ETag:         strings.Trim(resp.Header.Get("ETag"), "\""),
		Size:         size,
	}

	opt := storage.GetOption{}
	for _, optFn := range opts {
		optFn(&opt)
	}

	if opt.WithTagging {
		tagging, _, err := c.client.Object.GetTagging(ctx, objectKey)
		if err != nil {
			return nil, err
		}

		fileInfo.Tagging = cosTagsToMap(tagging.TagSet)
	}

	if opt.WithURL {
		fileInfo.URL, err = c.GetObjectUrl(ctx, objectKey, opts...)
		if err != nil {
			return nil, err
		}
	}

	return fileInfo, nil
}

func cosTagsToMap(tags []cosv5.ObjectTaggingTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.Key] = tag.Value
	}

	return m
}

func parseCosTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}

	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000Z",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}

	if t, err := http.ParseTime(value); err == nil {
		return t
	}

	return time.Time{}
}
