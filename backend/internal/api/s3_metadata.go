package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/services"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// S3 user-metadata and object-tagging support (x-amz-meta-*, x-amz-tagging,
// and the ?tagging subresource). The database is the source of truth for
// serving GET/HEAD; the S3 backend additionally receives metadata so objects
// stay self-describing when read directly from AWS.

const (
	amzMetaPrefix   = "x-amz-meta-"
	maxUserMetadata = 2 * 1024 // AWS limit: 2KB total user metadata
	maxObjectTags   = 10       // AWS limit: 10 tags per object
)

// extractUserMetadata collects x-amz-meta-* request headers into a map keyed
// by the lowercased suffix (AWS canonicalizes metadata names to lowercase).
// Returns an error when the aggregate size exceeds the AWS 2KB limit.
func extractUserMetadata(c *gin.Context) (map[string]string, error) {
	meta := map[string]string{}
	total := 0
	for name, vals := range c.Request.Header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, amzMetaPrefix) || len(vals) == 0 {
			continue
		}
		key := strings.TrimPrefix(lower, amzMetaPrefix)
		if key == "" {
			continue
		}
		val := vals[0]
		total += len(key) + len(val)
		if total > maxUserMetadata {
			return nil, fmt.Errorf("user metadata exceeds the 2KB limit")
		}
		meta[key] = val
	}
	if len(meta) == 0 {
		return nil, nil
	}
	return meta, nil
}

// parseTaggingHeader parses an x-amz-tagging header (URL query encoding,
// e.g. "env=prod&team=infra") into a tag map, enforcing AWS limits.
func parseTaggingHeader(raw string) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, fmt.Errorf("malformed x-amz-tagging header")
	}
	tags := map[string]string{}
	for k, v := range vals {
		if err := validateTag(k, v[0]); err != nil {
			return nil, err
		}
		tags[k] = v[0]
	}
	if len(tags) > maxObjectTags {
		return nil, fmt.Errorf("object may have at most %d tags", maxObjectTags)
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return tags, nil
}

func validateTag(k, v string) error {
	if k == "" || len(k) > 128 {
		return fmt.Errorf("tag key must be 1-128 characters")
	}
	if len(v) > 256 {
		return fmt.Errorf("tag value must be at most 256 characters")
	}
	return nil
}

// mapToJSONPtr serializes a map to a *string for a nullable jsonb column;
// nil/empty maps become nil (SQL NULL).
func mapToJSONPtr(m map[string]string) *string {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

// jsonPtrToMap parses a nullable jsonb column back into a map.
func jsonPtrToMap(p *string) map[string]string {
	if p == nil || *p == "" {
		return nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(*p), &m); err != nil {
		return nil
	}
	return m
}

// writeUserMetadataHeaders emits x-amz-meta-* (and the tag count) on GET/HEAD
// responses, matching S3 semantics.
func writeUserMetadataHeaders(c *gin.Context, obj *models.Object) {
	for k, v := range jsonPtrToMap(obj.Metadata) {
		// Bypass Go's header canonicalization (X-Amz-Meta-Env) — S3 returns
		// metadata names lowercased, and SDKs surface the suffix verbatim.
		c.Writer.Header()[amzMetaPrefix+k] = []string{v}
	}
	if tags := jsonPtrToMap(obj.Tags); len(tags) > 0 {
		c.Header("x-amz-tagging-count", fmt.Sprintf("%d", len(tags)))
	}
}

func writeStoredContentHeaders(c *gin.Context, disposition, cacheControl string) {
	if disposition != "" {
		c.Header("Content-Disposition", disposition)
	}
	if cacheControl != "" {
		c.Header("Cache-Control", cacheControl)
	}
}

// ── ?tagging subresource ─────────────────────────────────────────────────────

type taggingXML struct {
	XMLName xml.Name  `xml:"Tagging"`
	Xmlns   string    `xml:"xmlns,attr,omitempty"`
	TagSet  tagSetXML `xml:"TagSet"`
}

type tagSetXML struct {
	Tags []tagXML `xml:"Tag"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// loadObjectForTagging performs the shared auth + lookup for tagging ops.
func (h *S3APIHandler) loadObjectForTagging(c *gin.Context, action string) (*models.Object, bool) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return nil, false
	}

	// Reading tags is authorized as s3:GetObject, but public-read does not
	// cover it (AWS: s3:GetObjectTagging), so only policies decide.
	if allowed, _ := h.policyService.CheckObjectAccessWithoutPublicRead(userUUID, bucketName, objectKey, action); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return nil, false
	}
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return nil, false
	}
	var obj models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&obj).Error; err != nil {
		h.s3Error(c, "NoSuchKey", "The specified key does not exist", objectKey, http.StatusNotFound)
		return nil, false
	}
	// Tags are kept for the current version only: a ?versionId naming another
	// version must not read or change the current version's tags.
	if vid := c.Query("versionId"); vid != "" {
		cur := obj.VersionID
		if cur == "" {
			cur = "null"
		}
		if vid != cur {
			h.s3Error(c, "NotImplemented", "Tagging of non-current object versions is not supported by bkt", objectKey, http.StatusNotImplemented)
			return nil, false
		}
		c.Header("x-amz-version-id", vid)
	}
	return &obj, true
}

// GetObjectTagging handles GET /{bucket}/{key}?tagging
func (h *S3APIHandler) GetObjectTagging(c *gin.Context) {
	obj, ok := h.loadObjectForTagging(c, services.ActionGetObject)
	if !ok {
		return
	}
	out := taggingXML{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for k, v := range jsonPtrToMap(obj.Tags) {
		out.TagSet.Tags = append(out.TagSet.Tags, tagXML{Key: k, Value: v})
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, out)
}

// PutObjectTagging handles PUT /{bucket}/{key}?tagging
func (h *S3APIHandler) PutObjectTagging(c *gin.Context) {
	obj, ok := h.loadObjectForTagging(c, services.ActionPutObject)
	if !ok {
		return
	}
	body, err := readBoundedBody(c.Request.Body, 64*1024)
	if err != nil {
		h.s3Error(c, "InvalidRequest", "Failed to read request body", "", http.StatusBadRequest)
		return
	}
	var in taggingXML
	if err := xml.Unmarshal(body, &in); err != nil {
		h.s3Error(c, "MalformedXML", "The XML you provided was not well-formed", "", http.StatusBadRequest)
		return
	}
	if len(in.TagSet.Tags) > maxObjectTags {
		h.s3Error(c, "BadRequest", fmt.Sprintf("Object may have at most %d tags", maxObjectTags), "", http.StatusBadRequest)
		return
	}
	tags := map[string]string{}
	for _, t := range in.TagSet.Tags {
		if err := validateTag(t.Key, t.Value); err != nil {
			h.s3Error(c, "InvalidTag", err.Error(), "", http.StatusBadRequest)
			return
		}
		if _, dup := tags[t.Key]; dup {
			h.s3Error(c, "InvalidTag", "Duplicate tag keys are not allowed", "", http.StatusBadRequest)
			return
		}
		tags[t.Key] = t.Value
	}
	if err := database.DB.Model(obj).Updates(map[string]interface{}{
		"tags":       mapToJSONPtr(tags),
		"updated_at": time.Now(),
	}).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to store tags", "", http.StatusInternalServerError)
		return
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// DeleteObjectTagging handles DELETE /{bucket}/{key}?tagging
func (h *S3APIHandler) DeleteObjectTagging(c *gin.Context) {
	obj, ok := h.loadObjectForTagging(c, services.ActionPutObject)
	if !ok {
		return
	}
	if err := database.DB.Model(obj).Updates(map[string]interface{}{
		"tags":       nil,
		"updated_at": time.Now(),
	}).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to delete tags", "", http.StatusInternalServerError)
		return
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusNoContent)
}
