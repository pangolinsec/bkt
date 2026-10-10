package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/validation"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// S3 versioning API: PUT/GET ?versioning, GET ?versions, and the
// ?versionId-addressed object operations.

type versioningConfigXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

type listVersionsResult struct {
	XMLName             xml.Name          `xml:"ListVersionsResult"`
	Xmlns               string            `xml:"xmlns,attr"`
	Name                string            `xml:"Name"`
	Prefix              string            `xml:"Prefix"`
	KeyMarker           string            `xml:"KeyMarker"`
	VersionIdMarker     string            `xml:"VersionIdMarker"` //nolint:revive // S3 XML element name
	NextKeyMarker       string            `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string            `xml:"NextVersionIdMarker,omitempty"` //nolint:revive // S3 XML element name
	MaxKeys             int               `xml:"MaxKeys"`
	IsTruncated         bool              `xml:"IsTruncated"`
	Versions            []versionEntryXML `xml:"Version"`
	DeleteMarkers       []deleteMarkerXML `xml:"DeleteMarker"`
}

type versionEntryXML struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type deleteMarkerXML struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

// PutBucketVersioning handles PUT /{bucket}?versioning (admin, or
// s3:PutBucketVersioning on the bucket).
func (h *S3APIHandler) PutBucketVersioning(c *gin.Context) {
	bucketName := c.Param("bucket")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}
	if !authorizeBucketConfig(h.policyService, userUUID, bucket.Name, services.ActionPutBucketVersioning) {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return
	}

	body, err := readBoundedBody(c.Request.Body, 64*1024)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			h.s3Error(c, "MaxMessageLengthExceeded", "Your request was too big", "", http.StatusBadRequest)
			return
		}
		h.s3Error(c, "InvalidRequest", "Failed to read request body", "", http.StatusBadRequest)
		return
	}
	var cfg versioningConfigXML
	if err := xml.Unmarshal(body, &cfg); err != nil {
		h.s3Error(c, "MalformedXML", "The XML you provided was not well-formed", "", http.StatusBadRequest)
		return
	}
	var status string
	switch strings.ToLower(cfg.Status) {
	case "enabled":
		status = models.VersioningEnabled
	case "suspended":
		status = models.VersioningSuspended
	default:
		h.s3Error(c, "MalformedXML", "Status must be Enabled or Suspended", "", http.StatusBadRequest)
		return
	}
	unlockBucket, lockErr := lockBucketExclusive(c.Request.Context(), &bucket)
	if lockErr != nil {
		h.s3Error(c, "InternalError", "Failed to lock bucket", "", http.StatusInternalServerError)
		return
	}
	defer unlockBucket()
	if err := refreshLockedBucket(c.Request.Context(), &bucket); err != nil {
		h.s3Error(c, "InternalError", "Failed to reload bucket", "", http.StatusInternalServerError)
		return
	}
	if status == models.VersioningSuspended && bucket.RetentionDays > 0 {
		h.s3Error(c, "InvalidBucketState", "Versioning cannot be suspended while retention is set", bucketName, http.StatusConflict)
		return
	}
	if err := database.DB.Model(&bucket).Update("versioning", status).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to update versioning", "", http.StatusInternalServerError)
		return
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// ListObjectVersions handles GET /{bucket}?versions: every version of every
// key (the current object, archived versions and delete markers), keys in
// ascending order and each key's versions newest first, paginated with
// max-keys / key-marker / version-id-marker exactly like S3 (IsTruncated,
// NextKeyMarker, NextVersionIdMarker). IsLatest marks each key's newest entry.
func (h *S3APIHandler) ListObjectVersions(c *gin.Context) {
	bucketName := c.Param("bucket")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}
	if allowed, _ := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return
	}

	prefix := c.Query("prefix")
	keyMarker := c.Query("key-marker")
	versionMarker := c.Query("version-id-marker")
	if versionMarker != "" && keyMarker == "" {
		h.s3Error(c, "InvalidArgument", "A version-id marker cannot be specified without a key marker.", bucketName, http.StatusBadRequest)
		return
	}
	maxKeys := 1000
	if mk := c.Query("max-keys"); mk != "" {
		parsed, err := strconv.Atoi(mk)
		if err != nil || parsed < 0 {
			h.s3Error(c, "InvalidArgument", "Provided max-keys not an integer or within integer range", bucketName, http.StatusBadRequest)
			return
		}
		if parsed < maxKeys {
			maxKeys = parsed
		}
	}

	items, err := loadVersionListing(&bucket, prefix, keyMarker, versionMarker, maxKeys+1)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to list versions", "", http.StatusInternalServerError)
		return
	}
	page, truncated := paginateVersionItems(items, maxKeys)

	out := listVersionsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucketName,
		Prefix: prefix, KeyMarker: keyMarker, VersionIdMarker: versionMarker,
		MaxKeys: maxKeys, IsTruncated: truncated,
	}
	if truncated && len(page) > 0 {
		last := page[len(page)-1]
		out.NextKeyMarker = last.key
		out.NextVersionIdMarker = last.versionID
	}
	for _, it := range page {
		if it.isMarker {
			out.DeleteMarkers = append(out.DeleteMarkers, deleteMarkerXML{
				Key: it.key, VersionId: it.versionID, IsLatest: it.isLatest,
				LastModified: it.lastModified.UTC().Format(time.RFC3339),
			})
			continue
		}
		out.Versions = append(out.Versions, versionEntryXML{
			Key: it.key, VersionId: it.versionID, IsLatest: it.isLatest,
			LastModified: it.lastModified.UTC().Format(time.RFC3339),
			ETag:         `"` + it.etag + `"`, Size: it.size, StorageClass: "STANDARD",
		})
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, out)
}

// versionListItem is one entry of a version listing.
type versionListItem struct {
	key          string
	versionID    string
	isMarker     bool
	isLatest     bool
	lastModified time.Time
	etag         string
	size         int64
}

func currentVersionItem(o *models.Object) versionListItem {
	vid := o.VersionID
	if vid == "" {
		vid = "null"
	}
	return versionListItem{key: o.Key, versionID: vid, lastModified: o.UpdatedAt, etag: o.ETag, size: o.Size}
}

func archivedVersionItem(v *models.ObjectVersion) versionListItem {
	if v.IsDeleteMarker {
		return versionListItem{key: v.Key, versionID: v.VersionID, isMarker: true, lastModified: v.VersionedAt}
	}
	return versionListItem{key: v.Key, versionID: v.VersionID, lastModified: v.ContentModifiedAt, etag: v.ETag, size: v.Size}
}

// mergeVersionItems interleaves current rows and archived versions into
// listing order: keys in the order given, and per key the current version
// first (it is always the newest) followed by the archived versions in the
// order supplied (newest first). The first entry of each key is its latest
// version — the caller must only pass keys listed from their beginning.
func mergeVersionItems(keys []string, currents map[string]*models.Object, versions map[string][]models.ObjectVersion) []versionListItem {
	var out []versionListItem
	for _, k := range keys {
		first := true
		if cur := currents[k]; cur != nil {
			it := currentVersionItem(cur)
			it.isLatest = true
			out = append(out, it)
			first = false
		}
		for i := range versions[k] {
			it := archivedVersionItem(&versions[k][i])
			it.isLatest = first
			first = false
			out = append(out, it)
		}
	}
	return out
}

// paginateVersionItems cuts items to maxKeys, reporting truncation.
func paginateVersionItems(items []versionListItem, maxKeys int) ([]versionListItem, bool) {
	if len(items) > maxKeys {
		return items[:maxKeys], true
	}
	return items, false
}

// versionOrder is the newest-first order of a key's archived versions (the
// id tie-break makes it total, so version-id-marker resumes exactly).
const versionOrder = "versioned_at DESC, id DESC"

// loadVersionListing returns up to limit listing entries, starting after
// (keyMarker, versionMarker) — or after keyMarker's last version when
// versionMarker is empty. Memory is bounded by limit regardless of how many
// versions a bucket or a single key holds.
func loadVersionListing(bucket *models.Bucket, prefix, keyMarker, versionMarker string, limit int) ([]versionListItem, error) {
	var items []versionListItem
	likePrefix := ""
	if prefix != "" {
		likePrefix = validation.EscapeLikeWildcards(prefix) + "%"
	}

	// Resume inside keyMarker, after versionMarker. Nothing here is the
	// latest version of its key (the marker entry itself came earlier).
	if keyMarker != "" && versionMarker != "" {
		vq := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, keyMarker)
		resume := true
		var cur models.Object
		curErr := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, keyMarker).First(&cur).Error
		if curErr == nil && currentVersionItem(&cur).versionID == versionMarker {
			// Marker is the current version: every archived version follows.
		} else {
			var mv models.ObjectVersion
			if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, keyMarker, versionMarker).
				First(&mv).Error; err != nil {
				resume = false // unknown marker: continue with the next key
			} else {
				vq = vq.Where("(versioned_at < ?) OR (versioned_at = ? AND id < ?)", mv.VersionedAt, mv.VersionedAt, mv.ID)
			}
		}
		if resume {
			var rest []models.ObjectVersion
			if err := vq.Order(versionOrder).Limit(limit).Find(&rest).Error; err != nil {
				return nil, err
			}
			for i := range rest {
				items = append(items, archivedVersionItem(&rest[i]))
			}
		}
	}
	remaining := limit - len(items)
	if remaining <= 0 {
		return items, nil
	}

	// Following keys, each listed from its newest version. Every key has at
	// least one entry, so `remaining` keys and `remaining` version rows
	// suffice to produce `remaining` entries.
	keyFilter := "bucket_id = ? AND key > ?"
	args := []interface{}{bucket.ID, keyMarker}
	if likePrefix != "" {
		keyFilter += " AND key LIKE ?"
		args = append(args, likePrefix)
	}
	var keys []string
	if err := database.DB.Raw(
		"SELECT key FROM objects WHERE "+keyFilter+
			" UNION SELECT key FROM object_versions WHERE "+keyFilter+
			" ORDER BY key LIMIT ?",
		append(append(append([]interface{}{}, args...), args...), remaining)...,
	).Scan(&keys).Error; err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return items, nil
	}

	var curRows []models.Object
	if err := database.DB.Where("bucket_id = ? AND key IN ?", bucket.ID, keys).Find(&curRows).Error; err != nil {
		return nil, err
	}
	currents := make(map[string]*models.Object, len(curRows))
	for i := range curRows {
		currents[curRows[i].Key] = &curRows[i]
	}
	var verRows []models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key IN ?", bucket.ID, keys).
		Order("key ASC, " + versionOrder).Limit(remaining).Find(&verRows).Error; err != nil {
		return nil, err
	}
	versions := make(map[string][]models.ObjectVersion)
	for _, v := range verRows {
		versions[v.Key] = append(versions[v.Key], v)
	}
	return append(items, mergeVersionItems(keys, currents, versions)...), nil
}

// GetObjectVersionByID handles GET/HEAD /{bucket}/{key}?versionId=X for a
// non-current version (the current version is served by the normal path).
func (h *S3APIHandler) serveObjectVersion(c *gin.Context, bucket *models.Bucket, objectKey, versionID string, headOnly bool) {
	var ver models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, objectKey, versionID).
		First(&ver).Error; err != nil {
		h.s3Error(c, "NoSuchVersion", "The specified version does not exist", objectKey, http.StatusNotFound)
		return
	}
	if ver.IsDeleteMarker {
		c.Header("x-amz-delete-marker", "true")
		c.Header("x-amz-version-id", ver.VersionID)
		h.s3Error(c, "MethodNotAllowed", "The specified version is a delete marker", objectKey, http.StatusMethodNotAllowed)
		return
	}

	c.Header("Content-Type", ver.ContentType)
	writeStoredContentHeaders(c, ver.ContentDisposition, ver.CacheControl)
	c.Header("ETag", `"`+ver.ETag+`"`)
	c.Header("Last-Modified", ver.ContentModifiedAt.UTC().Format(http.TimeFormat))
	c.Header("x-amz-version-id", ver.VersionID)
	c.Header("x-amz-request-id", uuid.New().String())
	for k, v := range jsonPtrToMap(ver.Metadata) {
		c.Writer.Header()[amzMetaPrefix+k] = []string{v}
	}
	if headOnly {
		c.Header("Content-Length", strconv.FormatInt(ver.Size, 10))
		c.Status(http.StatusOK)
		return
	}

	backend, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}
	rc, err := getVersionBytes(backend, bucket.Name, &ver)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to read version", objectKey, http.StatusInternalServerError)
		return
	}
	defer rc.Close() //nolint:errcheck // best-effort close of read stream
	c.DataFromReader(http.StatusOK, ver.Size, ver.ContentType, io.LimitReader(rc, ver.Size), nil)
}
