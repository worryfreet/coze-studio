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
	"testing"

	cosv5 "github.com/tencentyun/cos-go-sdk-v5"
)

func TestBuildBaseURL(t *testing.T) {
	baseURL, err := buildBaseURL("demo-bucket", "https://cos.ap-beijing.myqcloud.com", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if baseURL.ServiceURL.Host != "cos.ap-beijing.myqcloud.com" {
		t.Fatalf("unexpected service host: %s", baseURL.ServiceURL.Host)
	}
	if baseURL.BucketURL.Host != "demo-bucket.cos.ap-beijing.myqcloud.com" {
		t.Fatalf("unexpected bucket host: %s", baseURL.BucketURL.Host)
	}

	baseURL, err = buildBaseURL("demo-bucket", "cos.ap-singapore.myqcloud.com", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if baseURL.ServiceURL.Scheme != "https" {
		t.Fatalf("scheme should default to https, got %s", baseURL.ServiceURL.Scheme)
	}
	if baseURL.BucketURL.Host != "demo-bucket.cos.ap-singapore.myqcloud.com" {
		t.Fatalf("unexpected bucket host when missing scheme: %s", baseURL.BucketURL.Host)
	}

	baseURL, err = buildBaseURL("demo-bucket", "", "ap-shanghai")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if baseURL.ServiceURL.Host != "cos.ap-shanghai.myqcloud.com" {
		t.Fatalf("unexpected default host: %s", baseURL.ServiceURL.Host)
	}
}

func TestParseCosTime(t *testing.T) {
	ts := "2025-01-02T03:04:05.000Z"
	parsed := parseCosTime(ts)
	if parsed.IsZero() {
		t.Fatalf("expected parsed time, got zero")
	}
	if parsed.UTC().Year() != 2025 {
		t.Fatalf("unexpected year: %d", parsed.UTC().Year())
	}

	httpTS := "Wed, 02 Oct 2002 08:00:00 GMT"
	if parseCosTime(httpTS).IsZero() {
		t.Fatalf("http time layout should be parsed")
	}

	if !parseCosTime("").IsZero() {
		t.Fatalf("empty string should return zero time")
	}
}

func TestCosTagsToMap(t *testing.T) {
	tags := []cosv5.ObjectTaggingTag{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	}
	tagMap := cosTagsToMap(tags)
	if len(tagMap) != 2 {
		t.Fatalf("unexpected tag map size: %d", len(tagMap))
	}
	if tagMap["k1"] != "v1" || tagMap["k2"] != "v2" {
		t.Fatalf("tag map values mismatch: %v", tagMap)
	}
}
