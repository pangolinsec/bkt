# bkt API Documentation

Complete API reference for bkt.

## Quick Reference

### Public Endpoints (No Authentication)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/register` | Register new account |
| POST | `/api/auth/login` | Login |
| POST | `/api/auth/refresh` | Refresh token |
| GET | `/api/auth/sso/config` | Get SSO configuration |
| GET | `/api/auth/google/login` | Initiate Google OAuth |
| GET | `/api/auth/google/callback` | Google OAuth callback |
| POST | `/api/auth/vault/login` | Vault JWT login (token in body) |
| GET | `/api/auth/vault/login` | Initiate OIDC browser SSO (Vault/Keycloak/any OIDC IdP) |
| GET | `/api/auth/vault/callback` | OIDC SSO callback |

### User Endpoints (Authentication Required)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/logout` | Logout |
| GET | `/api/users/me` | Get current user |
| PUT | `/api/users/me` | Update current user |
| GET | `/api/access-keys` | List access keys |
| POST | `/api/access-keys` | Create access key |
| DELETE | `/api/access-keys/:id` | Revoke access key |
| GET | `/api/access-keys/stats` | Get key stats |
| POST | `/api/sts/credentials` | Issue temporary S3 credentials (bkt-STS) |
| GET | `/api/buckets` | List buckets |
| GET | `/api/buckets/:name` | Get bucket (admin or any of `s3:ListBucket` / `s3:GetBucketLocation` / `s3:GetBucketPolicy`; includes `permissions`) |
| GET | `/api/buckets/:name/policy` | Get bucket policy (admin or `s3:GetBucketPolicy`; 404 when none) |
| GET | `/api/buckets/:name/objects` | List objects |
| POST | `/api/buckets/:name/objects` | Upload object |
| POST | `/api/buckets/:name/objects/async` | Upload async |
| POST | `/api/buckets/:name/objects/presign` | Generate presigned GET URL |
| GET | `/api/buckets/:name/object-versions` | List an object's versions |
| DELETE | `/api/buckets/:name/object-versions` | Permanently delete a version |
| POST | `/api/buckets/:name/objects/restore` | Restore an object version |
| PUT | `/api/buckets/:name/versioning` | Enable/suspend versioning (owner/admin) |
| PUT | `/api/buckets/:name/lifecycle` | Set lifecycle expiry (owner/admin) |
| PUT | `/api/buckets/:name/settings` | Update bucket settings (admin or per-field policy action; `is_public` admin only) |
| GET | `/api/buckets/:name/objects/*key` | Download object |
| HEAD | `/api/buckets/:name/objects/*key` | Head object |
| DELETE | `/api/buckets/:name/objects/*key` | Delete object |
| POST | `/api/buckets/:name/objects/move` | Move object |
| POST | `/api/buckets/:name/objects/rename` | Rename object |
| POST | `/api/buckets/:name/folders/move` | Move folder |
| GET | `/api/uploads` | List uploads |
| GET | `/api/uploads/:id/status` | Get upload status |
| GET | `/api/policies` | List policies |

### Admin Endpoints (Admin Required)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/users` | List all users |
| POST | `/api/users` | Create user |
| DELETE | `/api/users/:id` | Delete user |
| POST | `/api/users/:id/lock` | Lock user |
| POST | `/api/users/:id/unlock` | Unlock user |
| GET | `/api/users/:id/access-keys` | List user's keys |
| DELETE | `/api/users/:id/access-keys/:key_id` | Delete user's key |
| POST | `/api/buckets` | Create bucket |
| DELETE | `/api/buckets/:name` | Delete bucket |
| PUT | `/api/buckets/:name/policy` | Set bucket policy (audit: `bucket.policy.set`) |
| DELETE | `/api/buckets/:name/policy` | Delete bucket policy (idempotent; audit: `bucket.policy.delete`) |
| POST | `/api/policies` | Create policy |
| GET | `/api/policies/:id` | Get policy |
| PUT | `/api/policies/:id` | Update policy |
| DELETE | `/api/policies/:id` | Delete policy |
| POST | `/api/policies/users/:user_id/attach` | Attach policy |
| DELETE | `/api/policies/users/:user_id/detach/:policy_id` | Detach policy |
| GET | `/api/groups` | List groups |
| POST | `/api/groups` | Create group |
| DELETE | `/api/groups/:id` | Delete group |
| PUT | `/api/groups/:id/sso-groups` | Replace linked SSO (IdP) groups |
| POST | `/api/groups/:id/members` | Add group member |
| DELETE | `/api/groups/:id/members/:user_id` | Remove group member |
| POST | `/api/groups/:id/policies` | Attach policy to group |
| DELETE | `/api/groups/:id/policies/:policy_id` | Detach policy from group |
| GET | `/api/audit` | List audit logs (filterable, paginated) |
| GET | `/api/s3-configs` | List S3 configs |
| POST | `/api/s3-configs` | Create S3 config |
| GET | `/api/s3-configs/:id` | Get S3 config |
| PUT | `/api/s3-configs/:id` | Update S3 config |
| DELETE | `/api/s3-configs/:id` | Delete S3 config |

### S3-Compatible API (Access Key Auth)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List buckets |
| HEAD | `/:bucket` | Head bucket |
| GET | `/:bucket` | List objects (V1/V2), `?versions`, `?uploads`, `?policy`, `?policyStatus`, `?acl`, `?versioning`, `?lifecycle`, `?location`, `?encryption` and read-only stubs (see [bucket sub-resources](#bucket-sub-resources)) |
| POST | `/:bucket` | Bulk delete (`?delete`) |
| PUT | `/:bucket` | `?policy` / `?versioning` / `?lifecycle` config (bucket creation disabled) |
| DELETE | `/:bucket` | `?policy` / `?lifecycle` (delete config) |
| HEAD | `/:bucket/*key` | Head object (`?versionId`, `?partNumber=1`) |
| GET | `/:bucket/*key` | Get object (Range, `?versionId`, `?partNumber=1`, `response-*`, `?tagging`, `?acl` read-only, ListParts via `?uploadId`) |
| PUT | `/:bucket/*key` | Put object / CopyObject / UploadPart / UploadPartCopy / `?tagging` |
| POST | `/:bucket/*key` | CreateMultipartUpload (`?uploads`) / CompleteMultipartUpload (`?uploadId`) |
| DELETE | `/:bucket/*key` | Delete object (`?versionId`, `?tagging`, AbortMultipartUpload via `?uploadId`) |

Any other object sub-resource is `501 NotImplemented` — see [Object sub-resources](#object-sub-resources).

---

## Base URL

All API endpoints are prefixed with `/api` except for the S3-compatible API which uses the root path.

## Authentication

All protected endpoints require a JWT token in the Authorization header:

```
Authorization: Bearer <token>
```

Authentication endpoints are rate-limited to **5 requests per minute per IP** by default (configurable via `AUTH_RATE_LIMIT`; browser SSO endpoints allow 6x that to accommodate redirect flows).

---

## Authentication Endpoints

<details>
<summary><code>POST /api/auth/register</code> - Register new account</summary>

**Rate Limited:** Yes (5 req/min)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | Yes | Unique username |
| email | string | Yes | Valid email address |
| password | string | Yes | Minimum 8 characters |

**Response (201 Created):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "uuid",
    "username": "string",
    "email": "string",
    "is_admin": false,
    "created_at": "timestamp"
  }
}
```

**Error Codes:**
- `400` - Invalid request or validation failure
- `403` - Registration disabled by administrator
- `409` - Username or email already exists

</details>

<details>
<summary><code>POST /api/auth/login</code> - Authenticate user</summary>

**Rate Limited:** Yes (5 req/min)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | Yes | Username |
| password | string | Yes | Password |

**Response (200 OK):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "uuid",
    "username": "string",
    "email": "string",
    "is_admin": false,
    "created_at": "timestamp"
  }
}
```

**Error Codes:**
- `400` - Invalid request format
- `401` - Invalid credentials
- `403` - Account locked by administrator

</details>

<details>
<summary><code>POST /api/auth/refresh</code> - Refresh access token</summary>

**Rate Limited:** Yes (5 req/min)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| refresh_token | string | Yes | Valid refresh token |

**Response (200 OK):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs..."
}
```

**Rotation and reuse detection:** each successful refresh **rotates** the refresh token — the old one is revoked and a new pair is returned. Replaying an already-rotated refresh token is treated as a theft indicator: **all of that user's sessions are revoked** (and the event is audited as `auth.refresh_reuse`).

**Error Codes:**
- `400` - Invalid request format
- `401` - Invalid, expired, rotated, or revoked refresh token

</details>

<details>
<summary><code>POST /api/auth/logout</code> - Invalidate session</summary>

**Authentication:** Required

Revokes **both** the access token and its sibling refresh token (the refresh token's JTI is embedded in the access token, so the pair is revoked even if the client does not send the refresh token). Optionally accepts `{"refresh_token": "..."}` in the body to explicitly blacklist a refresh token as well.

**Response (200 OK):**
```json
{
  "message": "Successfully logged out"
}
```

</details>

<details>
<summary><code>GET /api/auth/sso/config</code> - Get SSO configuration</summary>

Returns the enabled SSO providers and their configuration. Use this to determine which login options to display in the UI.

**Response (200 OK):**
```json
{
  "google_enabled": true,
  "google_auth_url": "https://accounts.google.com/o/oauth2/v2/auth?client_id=...&redirect_uri=...&scope=openid%20email%20profile&response_type=code",
  "vault_enabled": true
}
```

**Response Fields:**
| Field | Type | Description |
|-------|------|-------------|
| google_enabled | boolean | Whether Google OAuth is configured |
| google_auth_url | string | Full OAuth URL for Google login (only if enabled) |
| vault_enabled | boolean | Whether Vault JWT is configured |

**Example:**
```bash
curl -k https://localhost:9443/api/auth/sso/config
```

</details>

<details>
<summary><code>GET /api/auth/google/login</code> - Initiate Google OAuth</summary>

Redirects the browser to Google's OAuth consent page. After user approval, Google redirects back to the callback URL.

**Response:** `302 Redirect` to Google OAuth consent page

**Error Codes:**
- `500` - Google SSO not configured

**Example (browser redirect):**
```
https://localhost:9443/api/auth/google/login
```

</details>

<details>
<summary><code>GET /api/auth/google/callback</code> - Google OAuth callback</summary>

Handles the OAuth callback from Google after user authentication. Creates or updates the user account and redirects to the frontend with tokens.

**Query Parameters (from Google):**
| Parameter | Type | Description |
|-----------|------|-------------|
| code | string | Authorization code from Google |
| state | string | CSRF state parameter |

**Success Behavior:**
- Creates user account if first login
- Updates user info on subsequent logins
- If Google Workspace enabled: syncs policies from user's groups, and memberships of bkt groups linked to those groups (`sso_groups`)
- Redirects to frontend with token in URL fragment

**Error Codes:**
- `400` - Invalid or missing authorization code
- `500` - Failed to exchange code for token

**Google Workspace Integration:**

Enable `GOOGLE_WORKSPACE_ENABLED=true` for automatic policy sync from Google Workspace groups. Requires a service account with domain-wide delegation.

> **Note:** See [SSO Setup Guide](../guides/sso-setup.md) for complete Google Workspace configuration.

</details>

<details>
<summary><code>POST /api/auth/vault/login</code> - Login with Vault JWT</summary>

Authenticate using a JWT token from HashiCorp Vault. Supports automatic policy assignment from JWT claims.

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| token | string | Yes | Vault-issued JWT token |

**JWT Claims:**
| Claim | Required | Description |
|-------|----------|-------------|
| sub | Yes | Unique user identifier |
| email | Yes | User's email address |
| name | No | Display name |
| groups | No | Group memberships (array; claim name set by `VAULT_JWT_GROUPS_CLAIM`). Syncs membership of bkt groups with linked `sso_groups`; absent = removed from all linked groups |
| policies | No | Policy names to assign (array) |

**Example JWT Payload:**
```json
{
  "sub": "12345-abcde-67890",
  "email": "alice@company.com",
  "name": "Alice Smith",
  "groups": ["engineering", "platform-team"],
  "policies": ["team-engineering-access", "project-alpha-readonly"]
}
```

**Response (200 OK):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "uuid",
    "username": "alice",
    "email": "alice@company.com",
    "is_admin": false,
    "sso_provider": "vault",
    "created_at": "timestamp"
  }
}
```

**Policy Sync Behavior:**
- Policies in the `policies` JWT claim are matched by name against existing policies
- On each login, user's policies are synced from SSO (SSO is source of truth)
- Unknown policy names in JWT are silently ignored
- Policy names are **case-sensitive** and must match exactly

**Error Codes:**
- `400` - Invalid or missing JWT token
- `401` - JWT validation failed (expired, invalid signature)
- `500` - Policy sync failed

**Example:**
```bash
curl -k -X POST https://localhost:9443/api/auth/vault/login \
  -H 'Content-Type: application/json' \
  -d '{
    "token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
  }'
```

> **Note:** See [SSO Setup Guide](../guides/sso-setup.md) for complete Vault JWT configuration.

</details>

<details>
<summary><code>GET /api/auth/vault/login</code> / <code>GET /api/auth/vault/callback</code> - OIDC browser SSO</summary>

Browser-based OIDC login with PKCE, configured via the `VAULT_OIDC_*` environment variables (`VAULT_OIDC_ENABLED`, `VAULT_OIDC_CLIENT_ID`, `VAULT_OIDC_PROVIDER_URL`, `VAULT_OIDC_REDIRECT_URL`, `VAULT_OIDC_SCOPES`). Despite the `VAULT_` prefix, this is a **generic OIDC** flow — endpoints are read from the provider's discovery document, so it works with any standards-compliant OIDC IdP (validated against HashiCorp Vault and Keycloak).

**Flow:**
1. `GET /api/auth/vault/login` redirects the browser to the IdP's authorization endpoint.
2. After authentication, the IdP redirects to `/api/auth/vault/callback`, which creates/updates the user, syncs policies from a `policies` claim, and returns tokens to the frontend.

All SSO logins (Google and OIDC) are recorded in the audit log with provider metadata.

</details>

---

## User Endpoints

<details>
<summary><code>GET /api/users/me</code> - Get current user profile</summary>

**Authentication:** Required

**Response (200 OK):**
```json
{
  "id": "uuid",
  "username": "string",
  "email": "string",
  "is_admin": false,
  "is_locked": false,
  "created_at": "timestamp",
  "updated_at": "timestamp"
}
```

</details>

<details>
<summary><code>PUT /api/users/me</code> - Update current user</summary>

**Authentication:** Required

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| email | string | No | New email address (local accounts only — SSO accounts get `403`; their address is managed by the IdP) |
| current_password | string | With `password` | Current password (re-authentication) |
| password | string | No | New password (min 8 chars; local accounts only) |
| revoke_access_keys | bool | No | With a password change: also deactivate all long-lived access keys (e.g. after a suspected compromise) |

A password change ends all sessions and revokes all temporary (bkt-STS) credentials.

**Response (200 OK):** Updated user object

**Error Codes:**
- `400` - Invalid email format, or `revoke_access_keys` without a password change
- `403` - SSO account tried to change email or password

</details>

<details>
<summary><code>GET /api/users</code> - List all users <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "username": "string",
    "email": "string",
    "is_admin": false,
    "is_locked": false,
    "created_at": "timestamp"
  }
]
```

</details>

<details>
<summary><code>POST /api/users</code> - Create user <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | Yes | Unique username |
| email | string | Yes | Valid email address |
| password | string | Yes | Minimum 8 characters |
| is_admin | boolean | No | Grant admin privileges (default: false) |

**Response (201 Created):** User object

**Error Codes:**
- `400` - Validation failed
- `409` - Username or email already exists

</details>

<details>
<summary><code>DELETE /api/users/:id</code> - Delete user <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | User ID |

Deleting also removes the user's access keys, policy/group attachments, and
their username from every bucket-policy `Principal` (statements that named only
this user are dropped), so a future account with the same username does not
inherit their bucket access.

**Response (200 OK):**
```json
{
  "message": "User deleted successfully"
}
```

**Error Codes:**
- `409` - Deleting yourself, deleting the last administrator, or the user still owns buckets (reassign or delete them first; the message lists them)

</details>

<details>
<summary><code>POST /api/users/:id/lock</code> - Lock user account <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | User ID |

**Response (200 OK):**
```json
{
  "message": "User locked successfully"
}
```

**Error Codes:**
- `403` - Cannot lock admin users

</details>

<details>
<summary><code>POST /api/users/:id/unlock</code> - Unlock user account <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | User ID |

**Response (200 OK):**
```json
{
  "message": "User unlocked successfully"
}
```

</details>

<details>
<summary><code>GET /api/users/:id/access-keys</code> - List user's access keys <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | User ID |

**Response (200 OK):** Array of access key objects

</details>

<details>
<summary><code>DELETE /api/users/:id/access-keys/:key_id</code> - Delete user's access key <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | User ID |
| key_id | UUID | Access key ID |

**Response (200 OK):**
```json
{
  "message": "Access key deleted successfully"
}
```

</details>

---

## Access Keys

Access keys are used for S3-compatible API authentication. Each user can have up to **5 active regular access keys** (temporary bkt-STS keys don't count against the limit).

<details>
<summary><code>POST /api/access-keys</code> - Generate access key</summary>

**Authentication:** Required

**Request Body (optional — an empty body creates an unnamed, non-expiring, full-access key):**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | No | Human-readable key name |
| read_only | boolean | No | Deny all mutating S3 operations (default: false) |
| expires_in_days | integer | No | Auto-expire after N days (0/omitted = never) |

**Response (201 Created):**
```json
{
  "message": "Access key created successfully",
  "access_key": "AKIA...",
  "secret_key": "wJalrXUtnFEMI...",
  "name": "ci-deploy",
  "read_only": false,
  "expires_at": "timestamp or null",
  "created_at": "timestamp",
  "warning": "Save your secret key now. It will not be shown again!"
}
```

> **Important:** The `secret_key` is only shown once at creation time.

**Error Codes:**
- `400` - Maximum access keys reached (5 active regular keys), or malformed body

</details>

<details>
<summary><code>GET /api/access-keys</code> - List access keys</summary>

**Authentication:** Required

Returns only the user's **active** regular keys (revoked keys are soft-deleted and hidden here; admins can see the full history via `GET /api/users/:id/access-keys`). Temporary bkt-STS keys are excluded.

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "access_key": "AKIA...XXXX",
    "name": "ci-deploy",
    "read_only": false,
    "is_active": true,
    "expires_at": "timestamp or null",
    "last_used_at": "timestamp",
    "created_at": "timestamp"
  }
]
```

</details>

<details>
<summary><code>DELETE /api/access-keys/:id</code> - Revoke access key</summary>

**Authentication:** Required

Revocation is a **soft delete**: the key stops working immediately but the record is kept for the audit trail (visible to admins via `GET /api/users/:id/access-keys`).

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Access key ID |

**Response (200 OK):**
```json
{
  "message": "Access key revoked successfully"
}
```

**Error Codes:**
- `403` - Cannot revoke another user's keys (unless admin)
- `404` - Key not found

</details>

<details>
<summary><code>GET /api/access-keys/stats</code> - Get access key statistics</summary>

**Authentication:** Required

**Response (200 OK):**
```json
{
  "active_keys": 2,
  "total_keys": 3,
  "max_keys": 5
}
```

</details>

<details>
<summary><code>POST /api/sts/credentials</code> - Issue temporary S3 credentials (bkt-STS)</summary>

**Authentication:** Required

Mints a short-lived S3 access key pair for the caller. Temporary keys are **excluded** from the 5-key limit and from the user's key list, and are automatically deleted after expiry.

> **Note:** bkt-STS is **not** AWS STS API-compatible (no `AssumeRole` etc.) — it is a simple REST endpoint that issues expiring key pairs.

**Request Body (optional):**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| duration_seconds | integer | No | Credential lifetime (default 3600 = 1h, max 43200 = 12h, min 60) |
| read_only | boolean | No | Deny all mutating S3 operations (default: false) |

**Response (201 Created):**
```json
{
  "access_key": "AK...",
  "secret_key": "SK...",
  "expires_at": "2026-09-02T13:00:00Z",
  "read_only": false
}
```

> The `secret_key` is shown only once. Issuance is audited as `sts.issue`.

</details>

---

## Buckets

<details>
<summary><code>GET /api/buckets</code> - List buckets</summary>

**Authentication:** Required

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "name": "my-bucket",
    "owner_id": "uuid",
    "is_public": false,
    "region": "us-east-1",
    "storage_backend": "local",
    "created_at": "timestamp",
    "updated_at": "timestamp"
  }
]
```

</details>

<details>
<summary><code>POST /api/buckets</code> - Create bucket <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | S3-compliant bucket name |
| region | string | Yes | AWS region (e.g., "us-east-1") |
| is_public | boolean | No | Public read access (default: false): unsigned GET/HEAD of objects on the S3 listener; listing and writes still require credentials. See [Public-read buckets](#public-read-buckets) |
| storage_backend | string | No | "local" or "s3" (default: "local") |
| s3_config_id | UUID | No | S3 configuration ID (if using S3 backend). Must exist (400 otherwise). If omitted, the current default S3 configuration is pinned to the bucket; with no default, the bucket uses the `.env` S3 settings. The bucket's routing never changes afterwards. |

**Bucket Naming Rules:**
- 3-63 characters
- Lowercase letters, numbers, and hyphens only
- Must start with a letter or number
- Cannot end with a hyphen
- No consecutive hyphens

**Response (201 Created):** Bucket object

**Error Codes:**
- `400` - Invalid bucket name or region
- `409` - Bucket already exists

</details>

<details>
<summary><code>GET /api/buckets/:name</code> - Get bucket details</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Authorization:** admin, or any of `s3:ListBucket`, `s3:GetBucketLocation`, `s3:GetBucketPolicy` on the bucket. Non-admins get a reduced view (owner id/username only, no `s3_config_id`; webhook and replication fields only with the permission to change them). `permissions` lists which settings the caller may change (`get_policy`, `put_policy`, `put_public_access`, `put_versioning`, `put_lifecycle`, `put_quota`, `put_retention`, `put_notification`, `put_replication`) — advisory, for the console.

**Response (200 OK):** Bucket object. For a public-read bucket it also includes `public_url_base` (`<S3 endpoint>/<bucket>`); append the key, each `/`-separated segment percent-encoded, to get the object's unsigned URL.

**Error Codes:**
- `403` - Permission denied
- `404` - Bucket not found

</details>

<details>
<summary><code>DELETE /api/buckets/:name</code> - Delete bucket <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Response (200 OK):**
```json
{
  "message": "Bucket deleted successfully"
}
```

**Error Codes:**
- `409` - S3 bucket not empty or its stored upstream route cannot be verified (including buckets created before route pinning)
- `500` - Upstream deletion or local filesystem cleanup failed; local cleanup is retried on startup

</details>

<details>
<summary><code>PUT /api/buckets/:name/policy</code> - Set bucket policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| policy | string | Yes | JSON policy document |

**Response (200 OK):**
```json
{
  "message": "Bucket policy set successfully"
}
```

The document is validated strictly (see [Policies](policies.md#policy-document-format)
and [bucket policies](policies.md#bucket-policies)); an invalid one answers
`400` with the validator's message. Audit-logged as `bucket.policy.set` (with
the new and previous document). Same operation as S3 `PUT /:bucket?policy`.

</details>

<details>
<summary><code>GET /api/buckets/:name/policy</code> - Get bucket policy</summary>

**Authentication:** Required — admin, or `s3:GetBucketPolicy` on the bucket (`403` otherwise)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Response (200 OK):**
```json
{
  "policy": "{\"Version\":\"2012-10-17\",...}"
}
```

**Error Codes:**
- `404` - The bucket has no policy

</details>

<details>
<summary><code>DELETE /api/buckets/:name/policy</code> - Delete bucket policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

Removes the bucket policy. Idempotent: deleting when there is none also
answers `200`. An actual removal is audit-logged as `bucket.policy.delete`
(with the previous document). Same operation as S3 `DELETE /:bucket?policy`.

**Response (200 OK):**
```json
{
  "message": "Bucket policy deleted"
}
```

**Error Codes:**
- `403` - Not an admin
- `404` - Bucket not found

</details>

---

## Objects

<details>
<summary><code>GET /api/buckets/:name/objects</code> - List objects</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| prefix | string | "" | Filter by key prefix |
| max-keys | integer | 1000 | Maximum objects (1-1000) |
| continuation-token | string | "" | Resume listing from a previous page's `next_continuation_token` |

**Response (200 OK):**
```json
{
  "bucket": "my-bucket",
  "objects": [
    {
      "id": "uuid",
      "key": "folder/file.txt",
      "size": 1024,
      "content_type": "text/plain",
      "etag": "d41d8cd98f00b204e9800998ecf8427e",
      "created_at": "timestamp",
      "updated_at": "timestamp"
    }
  ],
  "count": 1,
  "is_truncated": false,
  "next_continuation_token": ""
}
```

When `is_truncated` is `true`, pass `next_continuation_token` as `continuation-token` on the next request to fetch the following page.

</details>

<details>
<summary><code>POST /api/buckets/:name/objects</code> - Upload object (synchronous)</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Form Data:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| file | binary | Yes | File to upload |
| key | string | Yes | Object key/path |

**Response (200 OK):**
```json
{
  "message": "Object uploaded successfully",
  "bucket": "my-bucket",
  "key": "folder/file.txt",
  "size": 1024,
  "etag": "d41d8cd98f00b204e9800998ecf8427e",
  "content_type": "text/plain"
}
```

**Error Codes:**
- `400` - Missing key, invalid key, forbidden file type
- `413` - File too large

</details>

<details>
<summary><code>POST /api/buckets/:name/objects/async</code> - Upload object (asynchronous)</summary>

Recommended for large files with progress tracking.

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Form Data:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| file | binary | Yes | File to upload |
| key | string | Yes | Object key/path |

**Response (202 Accepted):**
```json
{
  "upload_id": "uuid",
  "status": "pending",
  "message": "Upload initiated. Use /api/uploads/{upload_id}/status to check progress."
}
```

</details>

<details>
<summary><code>GET /api/buckets/:name/objects/*key</code> - Download object</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |
| key | string | Object key (can include slashes) |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| download | boolean | false | Set "true" for attachment download |

**Response Headers:**
- `Content-Type`: Object's MIME type
- `Content-Length`: File size in bytes
- `ETag`: MD5 hash
- `Last-Modified`: Modification timestamp
- `Accept-Ranges`: bytes
- `Content-Disposition`: "inline" or "attachment"

**Response:** Binary file stream

</details>

<details>
<summary><code>HEAD /api/buckets/:name/objects/*key</code> - Get object metadata</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |
| key | string | Object key |

**Response Headers:** Same as GET (no body)

**Status Codes:**
- `200` - Object exists
- `404` - Not found

</details>

<details>
<summary><code>DELETE /api/buckets/:name/objects/*key</code> - Delete object</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |
| key | string | Object key |

**Response (200 OK):**
```json
{
  "message": "Object deleted successfully"
}
```

</details>

<details>
<summary><code>POST /api/buckets/:name/objects/move</code> - Move object</summary>

Move an object to a different location within the same bucket.

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| source_key | string | Yes | Current object key |
| destination_key | string | Yes | New object key |

**Response (200 OK):**
```json
{
  "message": "Object moved successfully",
  "object": { ... }
}
```

**Error Codes:**
- `400` - Source and destination are the same
- `404` - Source object not found
- `409` - Destination already exists

</details>

<details>
<summary><code>POST /api/buckets/:name/objects/rename</code> - Rename object</summary>

Rename an object within the same folder.

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| source_key | string | Yes | Full path to object |
| new_name | string | Yes | New filename (no slashes) |

**Response (200 OK):**
```json
{
  "message": "Object renamed successfully",
  "object": { ... }
}
```

**Error Codes:**
- `400` - New name contains slashes
- `409` - Object with new name already exists

</details>

<details>
<summary><code>POST /api/buckets/:name/folders/move</code> - Move folder</summary>

Recursively move all objects with a prefix.

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| name | string | Bucket name |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| source_prefix | string | Yes | Source folder prefix (e.g., "folder1/") |
| destination_prefix | string | Yes | Destination folder prefix (e.g., "folder2/") |

**Response (200 OK):**
```json
{
  "message": "Folder moved successfully",
  "moved_count": 15
}
```

A move handles at most 10,000 objects. It requires `s3:GetObject` +
`s3:DeleteObject` on every source key and `s3:PutObject` on every destination
key, and never overwrites existing destination objects. It emits at most
1,000 webhook events (created/removed pairs for the first 500 objects); the
rest are skipped and the skip is logged.

**Error Codes:**
- `409` - A destination object exists, the source changed during the move,
  or some of the folder's objects are being written right now ("Objects
  busy" — the move waits at most 10 s for them; retry)

</details>

<details>
<summary><code>POST /api/buckets/:name/objects/presign</code> - Generate presigned GET URL</summary>

Issues a time-limited presigned download URL for an object, signed with one of the caller's **long-lived access keys** (SigV4) — active, not expired, non-expiring first, then the longest-lived. STS temporary credentials are never used (they die with the session, which would silently break the link); with no usable key the request returns 409. The URL points at the S3 API listener (`S3_PUBLIC_ENDPOINT`, or derived from the request host + S3 API port).

**Authentication:** Required (and at least one active access key)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| key | string | Yes | Object key |
| expires_in | integer | No | URL lifetime in seconds (default 900; min 60, max 604800 = 7 days) |

**Response (200 OK):**
```json
{
  "url": "https://host:9000/my-bucket/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&...",
  "expires_at": "2026-09-02T13:00:00Z",
  "capped_by_key": false,
  "signing_key_name": "my-key"
}
```

`capped_by_key` is `true` when the signing key expires before the requested lifetime — the URL's effective expiry is capped by the key's expiry.

**Error Codes:**
- `403` - No read permission on the object
- `404` - Bucket or object not found
- `409` - No active (or no sufficiently long-lived) access key

</details>

<details>
<summary><code>GET /api/buckets/:name/object-versions?key=K</code> - List an object's versions</summary>

> **Note the path:** `/object-versions`, **not** `/objects/versions` (a static segment under `/objects/` would conflict with the download wildcard route).

**Authentication:** Required

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| key | string | Yes | Object key |

**Response (200 OK):**
```json
{
  "bucket": "my-bucket",
  "key": "file.txt",
  "versioning": "enabled",
  "versions": [
    {
      "version_id": "uuid",
      "is_latest": true,
      "is_delete_marker": false,
      "size": 1024,
      "content_type": "text/plain",
      "etag": "d41d8cd98f00b204e9800998ecf8427e",
      "last_modified": "timestamp"
    }
  ]
}
```

Objects written before versioning was enabled report `version_id` `"null"`.

</details>

<details>
<summary><code>DELETE /api/buckets/:name/object-versions?key=K&version_id=V</code> - Permanently delete a version</summary>

Permanently removes one version (or delete marker). Removing the current version or a latest delete marker **promotes** the next-newest surviving version to current.

**Authentication:** Required

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| key | string | Yes | Object key |
| version_id | string | Yes | Version ID to remove |

**Response (200 OK):**
```json
{
  "message": "Version deleted"
}
```

</details>

<details>
<summary><code>POST /api/buckets/:name/objects/restore</code> - Restore an object version</summary>

Copies an archived version **forward** as the new current version. The previous current version is archived first — history is preserved. Delete markers cannot be restored.

**Authentication:** Required

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| key | string | Yes | Object key |
| version_id | string | Yes | Version to restore |

**Response (200 OK):**
```json
{
  "message": "Version restored"
}
```

</details>

---

## Bucket Versioning, Lifecycle, and Settings

<details>
<summary><code>PUT /api/buckets/:name/versioning</code> - Set bucket versioning <strong>[Owner/Admin]</strong></summary>

**Authentication:** Required (bucket owner or admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| versioning | string | Yes | `"enabled"` or `"suspended"` |

**Response (200 OK):**
```json
{
  "message": "Versioning enabled"
}
```

**Error Codes:**
- `400` - Value other than `enabled`/`suspended`
- `403` - Not the bucket owner or an admin
- `409` - Versioning cannot be suspended while a retention period is set

</details>

<details>
<summary><code>PUT /api/buckets/:name/lifecycle</code> - Set bucket lifecycle <strong>[Owner/Admin]</strong></summary>

Configures age-based expiry (single rule per bucket). Setting both day counts to zero clears the configuration.

**Authentication:** Required (bucket owner or admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| expire_days | integer | No | Expire current objects older than N days (versioned buckets get delete markers) |
| prefix | string | No | Only apply to keys with this prefix |
| noncurrent_expire_days | integer | No | Permanently remove noncurrent versions older than N days |

**Response (200 OK):**
```json
{
  "message": "Lifecycle updated"
}
```

Rules are applied hourly (and once shortly after startup).

</details>

<details>
<summary><code>PUT /api/buckets/:name/settings</code> - Update bucket settings</summary>

Partial update — only fields present in the body are changed.

**Authentication:** Required. Each field needs admin or its bucket-configuration action (`quota_bytes`: `s3:PutBucketQuota`, `retention_days`: `s3:PutBucketObjectLockConfiguration`, `webhook_*`: `s3:PutBucketNotification`, `replicate_to`: `s3:PutReplicationConfiguration`); `is_public` is **admin only**.

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| quota_bytes | integer | No | Max bytes of current objects (0 = unlimited; version storage doesn't count) |
| retention_days | integer | No | WORM-lite retention window (0 = off; **requires versioning enabled**) |
| webhook_url | string | No | http(s) URL to POST object events to ("" disables) |
| webhook_secret | string | No | HMAC-SHA256 secret for the `X-Bkt-Signature` header |
| webhook_events | string | No | CSV filter: `"created"`, `"removed"`, or both ("" = both) |
| replicate_to | string | No | Target bucket name for one-way replication ("" disables) |
| is_public | boolean | No | Public read access on/off ([Public-read buckets](#public-read-buckets)). **Admin only**; audit-logged as `bucket.public_access` (`old`/`new`) |

**Response (200 OK):**
```json
{
  "message": "Settings updated"
}
```

**Error Codes:**
- `400` - Negative values, non-http(s) webhook URL, retention without versioning, self-replication, or replication cycle
- `403` - Missing the permission for a requested field (`is_public`: caller is not an admin)
- `404` - Replication target bucket not found
- `409` - Replication target already mirrored by another bucket

</details>

---

## Upload Status

<details>
<summary><code>GET /api/uploads</code> - List uploads</summary>

**Authentication:** Required

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| status | string | all | Filter: "pending", "processing", "completed", "failed" |
| limit | integer | 50 | Maximum results (1-100) |

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "status": "processing",
    "filename": "large-file.zip",
    "object_key": "uploads/large-file.zip",
    "total_size": 104857600,
    "uploaded_size": 52428800,
    "progress_percent": 50.0,
    "error_message": null,
    "object_id": null,
    "created_at": "timestamp",
    "completed_at": null
  }
]
```

</details>

<details>
<summary><code>GET /api/uploads/:id/status</code> - Get upload status</summary>

**Authentication:** Required

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Upload ID |

**Response (200 OK):** Upload status object

**Error Codes:**
- `404` - Upload not found or doesn't belong to user

</details>

---

## Policies

<details>
<summary><code>GET /api/policies</code> - List policies</summary>

Admins see all policies; users see only their attached policies.

**Authentication:** Required

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "name": "ReadOnlyAccess",
    "description": "Read-only access to all buckets",
    "document": "{\"Version\":\"2012-10-17\",...}",
    "created_at": "timestamp",
    "updated_at": "timestamp"
  }
]
```

</details>

<details>
<summary><code>POST /api/policies</code> - Create policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | Policy name |
| description | string | No | Policy description |
| document | string | Yes | JSON policy document |

**Policy Document Format:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::my-bucket/*"]
    }
  ]
}
```

**Response (201 Created):** Policy object

</details>

<details>
<summary><code>GET /api/policies/:id</code> - Get policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Policy ID |

**Response (200 OK):** Policy object

</details>

<details>
<summary><code>PUT /api/policies/:id</code> - Update policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Policy ID |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | No | Policy name |
| description | string | No | Policy description |
| document | string | No | JSON policy document |

**Response (200 OK):** Updated policy object

</details>

<details>
<summary><code>DELETE /api/policies/:id</code> - Delete policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Policy ID |

**Response (200 OK):**
```json
{
  "message": "Policy deleted successfully"
}
```

**Error Codes:**
- `409` - Policy is attached to users (detach first)

</details>

<details>
<summary><code>POST /api/policies/users/:user_id/attach</code> - Attach policy to user <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| user_id | UUID | User ID |

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| policy_id | UUID | Yes | Policy ID to attach |

**Response (200 OK):**
```json
{
  "message": "Policy attached successfully"
}
```

</details>

<details>
<summary><code>DELETE /api/policies/users/:user_id/detach/:policy_id</code> - Detach policy <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| user_id | UUID | User ID |
| policy_id | UUID | Policy ID |

**Response (200 OK):**
```json
{
  "message": "Policy detached successfully"
}
```

</details>

---

## Groups

Groups are named sets of users that policies can attach to (admin only). A user's **effective policies = their direct policies ∪ the policies of every group they belong to**. See [Policies API](policies.md) for details.

<details>
<summary><code>GET /api/groups</code> - List groups <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Response (200 OK):** Array of group objects, each including its `users` (members; SSO users carry `sso_provider`, local users omit it), `policies`, and `sso_groups` (linked identity-provider group names, `[]` when none).

```json
[
  {
    "id": "…",
    "name": "engineering",
    "sso_groups": ["engineering", "platform-team"],
    "users": [{"id": "…", "username": "alice", "sso_provider": "oidc"}],
    "policies": [{"id": "…", "name": "eng-buckets"}]
  }
]
```

</details>

<details>
<summary><code>POST /api/groups</code> - Create group <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | Group name (2-64 characters, unique) |
| description | string | No | Group description |
| sso_groups | string[] | No | Identity-provider group names to link (see `PUT /api/groups/:id/sso-groups` for the rules) |

**Response (201 Created):** Group object

**Error Codes:**
- `400` - Invalid `sso_groups`
- `409` - Group name already exists

</details>

<details>
<summary><code>DELETE /api/groups/:id</code> - Delete group <strong>[Admin]</strong></summary>

Removes the group, its memberships, its policy attachments, and its SSO group links. Users and policies themselves are untouched.

**Response (200 OK):**
```json
{
  "message": "Group deleted"
}
```

</details>

<details>
<summary><code>PUT /api/groups/:id/sso-groups</code> - Replace linked SSO groups <strong>[Admin]</strong></summary>

Replaces the identity-provider group names linked to the group. SSO users whose IdP groups match any link (case-insensitive, exact) are added to the group at each sign-in and removed when they no longer match; groups without links, and local users, are never changed by sign-in. `[]` unlinks the group. Memberships change at each user's next sign-in. See [Mapping IdP groups to bkt groups](../guides/sso-setup.md#mapping-idp-groups-to-bkt-groups).

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| sso_groups | string[] | Yes | Names are trimmed, must be non-empty and ≤256 characters; duplicates are dropped case-insensitively; at most 100 |

**Response (200 OK):** Group object with the normalized `sso_groups`. The change is audit-logged as `group.sso_groups_update`.

**Error Codes:**
- `400` - Missing `sso_groups`, empty/too-long name, or more than 100 names
- `403` - Not an admin
- `404` - Group not found

</details>

<details>
<summary><code>POST /api/groups/:id/members</code> - Add member <strong>[Admin]</strong></summary>

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| user_id | UUID | Yes | User to add |

**Response (200 OK):** `{"message": "Member added"}` (idempotent — adding an existing member is a no-op). Note: for an SSO user in a group with linked SSO groups, the membership is re-evaluated (and possibly removed) at the user's next sign-in.

</details>

<details>
<summary><code>DELETE /api/groups/:id/members/:user_id</code> - Remove member <strong>[Admin]</strong></summary>

**Response (200 OK):** `{"message": "Member removed"}`

</details>

<details>
<summary><code>POST /api/groups/:id/policies</code> - Attach policy to group <strong>[Admin]</strong></summary>

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| policy_id | UUID | Yes | Policy to attach |

**Response (200 OK):** `{"message": "Policy attached"}`

</details>

<details>
<summary><code>DELETE /api/groups/:id/policies/:policy_id</code> - Detach policy from group <strong>[Admin]</strong></summary>

**Response (200 OK):** `{"message": "Policy detached"}`

</details>

---

## Audit Log

<details>
<summary><code>GET /api/audit</code> - List audit logs <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

Returns audit log entries, newest first. Covers logins (including SSO logins with provider metadata), group operations, `sts.issue`, `bucket.settings` / `bucket.public_access` / `bucket.versioning` / `bucket.lifecycle`, and more. Anonymous public-read downloads are not audited (they appear in the access log and metrics). Retention is controlled by `AUDIT_RETENTION_DAYS` (default 90; <=0 disables pruning).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| user_id | UUID | - | Filter by user |
| action | string | - | Filter by action (e.g. `auth.login`, `sts.issue`) |
| resource_type | string | - | Filter by resource type (e.g. `bucket`, `user`, `group`) |
| status | string | - | `success`, `failure`, or `denied` |
| start | RFC3339 | - | Entries at or after this time |
| end | RFC3339 | - | Entries at or before this time |
| limit | integer | 100 | Max results (max 500) |
| offset | integer | 0 | Pagination offset |

**Response (200 OK):**
```json
{
  "logs": [ ... ],
  "limit": 100,
  "offset": 0
}
```

</details>

---

## S3 Configurations

Manage external S3-compatible storage backends (admin only).

<details>
<summary><code>GET /api/s3-configs</code> - List S3 configurations <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Response (200 OK):**
```json
[
  {
    "id": "uuid",
    "name": "AWS Production",
    "endpoint": "s3.amazonaws.com",
    "region": "us-east-1",
    "access_key_id": "AKIA...",
    "bucket_prefix": "prod-",
    "use_ssl": true,
    "force_path_style": false,
    "is_default": true,
    "created_at": "timestamp"
  }
]
```

</details>

<details>
<summary><code>POST /api/s3-configs</code> - Create S3 configuration <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | Configuration name |
| endpoint | string | Yes | S3 endpoint (e.g., "s3.amazonaws.com") |
| region | string | Yes | AWS region |
| access_key_id | string | Yes | AWS access key ID |
| secret_access_key | string | Yes | AWS secret access key |
| bucket_prefix | string | No | Prefix for bucket names |
| use_ssl | boolean | No | Use HTTPS (default: true) |
| force_path_style | boolean | No | Use path-style URLs (default: false) |
| is_default | boolean | No | Set as default configuration |

> Credentials are encrypted before storage.

**Response (201 Created):** S3 configuration object

**Error Codes:**
- `409` - Configuration name already exists

</details>

<details>
<summary><code>GET /api/s3-configs/:id</code> - Get S3 configuration <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Configuration ID |

**Response (200 OK):** S3 configuration object

</details>

<details>
<summary><code>PUT /api/s3-configs/:id</code> - Update S3 configuration <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Configuration ID |

**Request Body:** Same as create (all fields optional). Omitted fields are
unchanged; `bucket_prefix: ""` clears the prefix. An `access_key_id` equal to
the stored (encrypted) value returned by GET is treated as unchanged.

Changing `endpoint`, `region`, `bucket_prefix`, `use_ssl` or
`force_path_style` is refused while any bucket uses the configuration (it
would silently re-route those buckets' data). Rotating credentials, renaming
and changing `is_default` are always allowed; `is_default` only affects
buckets created afterwards.

**Response (200 OK):** Updated configuration object

**Error Codes:**
- `409` - A location setting was changed while buckets use the configuration (the message lists them)

</details>

<details>
<summary><code>DELETE /api/s3-configs/:id</code> - Delete S3 configuration <strong>[Admin]</strong></summary>

**Authentication:** Required (Admin)

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | UUID | Configuration ID |

**Response (200 OK):**
```json
{
  "message": "S3 configuration deleted successfully"
}
```

**Error Codes:**
- `409` - Configuration is in use by buckets

</details>

---

## S3-Compatible API

The S3-compatible API enables tools like `s3fs-fuse`, AWS CLI, and other S3 clients to interact with bkt.

> **Endpoint:** The S3 API listens on its own port — **`9000`** by default —
> separate from the console / REST API (`9443`). Point your S3 client at
> `https://<host>:9000` (e.g. `aws --endpoint-url https://localhost:9000 s3 ls`).
> S3 clients address buckets at the host root, which is why this is a dedicated port.

### Authentication

Uses **AWS Signature V4** with access keys generated from the bkt API.

**Required Headers:**
- `Authorization`: AWS4-HMAC-SHA256 signature
- `X-Amz-Date`: Request timestamp
- `X-Amz-Content-Sha256`: Content hash

**Presigned URLs** (query-string SigV4, e.g. from `aws s3 presign` or `POST /api/buckets/:name/objects/presign`) are also verified and accepted, subject to the signing key's status and expiry. A presigned `X-Amz-Date` more than 15 minutes in the future is rejected.

Canonicalization follows AWS exactly (path segments and query keys/values URI-encoded per RFC 3986, space as `%20`), so keys with spaces, `+`, `%` or non-ASCII characters verify as signed by any AWS SDK. When `X-Amz-Content-Sha256` is a hex digest, the body is hashed as it is received and the request fails with `XAmzContentSHA256Mismatch` if it does not match; `UNSIGNED-PAYLOAD` and the `STREAMING-*` values are accepted (streaming chunks are verified by the chunk decoder). Any other value is rejected. Temporary (bkt-STS) keys stop working as soon as the issuing user's sessions are revoked.

Optional per-IP rate limiting on this listener is available via `S3_RATE_LIMIT` (requests/minute; 0 = disabled).

### Public-read buckets

On a bucket with `is_public: true`, **unsigned** `GET` and `HEAD` of an object (`/<bucket>/<key>`, current version, no query parameters other than `response-content-type`, `response-content-disposition`, `response-cache-control`, `response-expires`, `response-content-language`, `response-content-encoding`) are served without credentials; `Range` works. Everything else — listing (`GET /<bucket>`, `GET /<bucket>/`), `ListBuckets`, `HeadBucket`, writes, deletes, multipart, `?versionId` and every other sub-resource — still requires a signature and answers an unsigned request with the usual `401 AccessDenied` ("Missing authorization header"), also for private and non-existent buckets. A bucket-policy `Deny` for `s3:GetObject` with `Principal: "*"` (or none) still blocks anonymous reads (`403 AccessDenied`); Allow statements are irrelevant to anonymous readers. `.bkt-versions/` keys answer 404. Active content is still served as an attachment with `nosniff`, and an anonymous `response-content-disposition=inline` cannot override that. Unsigned reads are limited per client IP by `PUBLIC_READ_RATE_LIMIT` (requests/minute, default 600, `0` disables; excess → `503 SlowDown`). Signed requests on public buckets are authorized by policies exactly as before.

```bash
curl https://s3.example.com/my-public-bucket/images/logo.png
```

### Supported Operations

| Area | Supported |
|------|-----------|
| Buckets | ListBuckets, HeadBucket, GetBucketLocation |
| Listing | ListObjects (V1 and V2) with prefixes, delimiters, and pagination |
| Objects | GetObject (incl. Range requests), PutObject, HeadObject, DeleteObject, CopyObject (`x-amz-copy-source`), DeleteObjects (bulk `POST /:bucket?delete`) |
| Multipart | CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads |
| Metadata | `x-amz-meta-*` user metadata (2KB total limit) |
| Tagging | `x-amz-tagging` header + `?tagging` subresource (GET/PUT/DELETE) |
| Versioning | `?versioning` (GET/PUT), `?versions` (ListObjectVersions), `?versionId` on GET/HEAD/DELETE |
| Lifecycle | `?lifecycle` (GET/PUT/DELETE, single rule) |
| Bucket policy | `?policy` (GET/PUT/DELETE), `?policyStatus` (GET) — see [bucket policies](policies.md#bucket-policies-over-the-s3-api) |
| Bucket info (read-only) | `?acl`, `?encryption`, and AWS-shaped "not configured" answers for other sub-resources (below) |

**Not implemented:** the AWS STS API (`AssumeRole` etc. — see bkt-STS above for temporary credentials), object-lock headers/API (bkt retention is a bucket setting), bucket notifications via MQTT/Kafka, multi-rule lifecycle, and bucket creation via the S3 API (buckets are created in the console). `PUT /:bucket` on an **existing** bucket answers like AWS — `409 BucketAlreadyOwnedByYou` when the caller is an admin or has `s3:ListBucket`/`s3:PutObject` on it (rclone and the SDKs treat this as success), `409 BucketAlreadyExists` otherwise; for a new name it returns `403 AccessDenied`.

### Bucket sub-resources

Every bucket-level request (`/:bucket` and `/:bucket/`) is dispatched on its
query sub-resource. Nothing unrecognized ever falls through to a listing or to
the bucket-creation probe: an unsupported sub-resource answers
`501 NotImplemented` (S3 XML error). All of these need a signature (unsigned →
the usual `401`); the GETs below (except `?policy`) additionally need
`s3:ListBucket` on the bucket (or admin), like listing it.

| Sub-resource | GET | PUT | DELETE |
|---|---|---|---|
| `?policy` | 200 policy JSON / 404 `NoSuchBucketPolicy` (admin or `s3:GetBucketPolicy`) | 204 (admin) | 204, idempotent (admin) |
| `?policyStatus` | 200 `<PolicyStatus><IsPublic>` = public read access flag | 501 | 501 |
| `?acl` | 200 read-only `AccessControlPolicy`: owner `FULL_CONTROL`, plus `READ` for `AllUsers` while public read access is on | 501 (public read is a console setting) | 501 |
| `?location` | 200 `LocationConstraint` | 501 | 501 |
| `?versioning` | 200 | 200 | 501 |
| `?lifecycle` | 200 / 404 `NoSuchLifecycleConfiguration` | 200 | 204 |
| `?encryption` | 200 SSE-S3 (`AES256`) for S3-backed buckets when `S3_SSE=true`, else 404 `ServerSideEncryptionConfigurationNotFoundError` | 501 | 501 |
| `?tagging` | 404 `NoSuchTagSet` (no bucket tags) | 501 | 501 |
| `?cors` | 404 `NoSuchCORSConfiguration` | 501 | 501 |
| `?website` | 404 `NoSuchWebsiteConfiguration` | 501 | 501 |
| `?replication` | 404 `ReplicationConfigurationNotFoundError` (bkt replication is a console setting) | 501 | 501 |
| `?ownershipControls` | 404 `OwnershipControlsNotFoundError` | 501 | 501 |
| `?publicAccessBlock` | 404 `NoSuchPublicAccessBlockConfiguration` | 501 | 501 |
| `?object-lock` | 404 `ObjectLockConfigurationNotFoundError` (bkt retention is a console setting) | 501 | 501 |
| `?logging` | 200 empty `BucketLoggingStatus` | 501 | 501 |
| `?notification` | 200 empty `NotificationConfiguration` (webhooks are a console setting) | 501 | 501 |
| `?accelerate` | 200 empty `AccelerateConfiguration` | 501 | 501 |
| `?requestPayment` | 200 `Payer` = `BucketOwner` | 501 | 501 |
| anything else | 501 `NotImplemented` | 501 | 501 |

`POST /:bucket` supports only `?delete` (DeleteObjects); anything else is 501.
A plain `DELETE /:bucket` (DeleteBucket) stays `403` — buckets are deleted in
the console.

### Object sub-resources

Object requests (`/:bucket/<key>`) are dispatched the same way: an
unsupported sub-resource is `501 NotImplemented` (HEAD: empty body) and never
returns object content, writes, or deletes anything.

| Request | Behavior |
|---|---|
| GET / HEAD (plain) | Accepts only `versionId`, `partNumber`, the `response-*` overrides (GET), and signing parameters (`X-Amz-*`, `x-id`) |
| GET / HEAD `?partNumber=N` | bkt objects are single-part (multipart uploads are assembled on completion): `N=1` is the whole object (GET: `206` + `Content-Range`), `N>1` → `416 InvalidPartNumber`; with a `Range` header → `400` |
| GET `?acl` | `200` read-only `AccessControlPolicy`: bucket owner `FULL_CONTROL`, plus `AllUsers READ` for the current version on a public-read bucket. Same authorization as reading the object (`s3:GetObject`, public-read included; `?versionId` needs a grant). `404 NoSuchKey` / `NoSuchVersion` |
| GET/PUT/DELETE `?tagging` | Get / replace / delete the current version's tags. `?versionId` naming another version → `501` |
| GET `?uploadId` | ListParts |
| PUT `?partNumber&uploadId` | UploadPart / UploadPartCopy (with `x-amz-copy-source`); only one of them, or an empty value → `400 InvalidArgument` |
| PUT with `x-amz-copy-source` | CopyObject |
| PUT `?acl` | `501` (bkt has no object ACLs; public read is the bucket's *Public read access* setting) |
| POST `?uploads` / `?uploadId` | CreateMultipartUpload / CompleteMultipartUpload |
| DELETE `?uploadId` / `?versionId` | AbortMultipartUpload / delete that version |
| `?attributes`, `?retention`, `?legal-hold`, `?torrent`, `?restore`, `?select`, any other key | `501 NotImplemented` |

Unsigned requests are unaffected: only plain object GET/HEAD with `response-*`
parameters are ever anonymous on a public-read bucket; every sub-resource,
including `?acl` and `?partNumber`, still answers `401`.

A bucket `GET` is a listing only when every query parameter is one that
ListObjects / ListObjectsV2 / ListObjectVersions / ListMultipartUploads use:
`list-type`, `prefix`, `delimiter`, `marker`, `max-keys`,
`continuation-token`, `start-after`, `fetch-owner`, `encoding-type`,
`versions`, `key-marker`, `version-id-marker`, `uploads`, `upload-id-marker`,
`max-uploads`, MinIO's `metadata`, plus signing parameters (`X-Amz-*`) and the
SDKs' `x-id`. Any other parameter answers 501 instead of a listing.

<details>
<summary><code>GET /</code> - List buckets (S3)</summary>

**Response (200 OK):** XML
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Owner>
    <ID>user-uuid</ID>
    <DisplayName>username</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>my-bucket</Name>
      <CreationDate>2024-01-15T10:30:00Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
```

</details>

<details>
<summary><code>HEAD /:bucket</code> - Check bucket exists (S3)</summary>

**Status Codes:**
- `200` - Bucket exists
- `403` - Access denied
- `404` - Bucket not found

</details>

<details>
<summary><code>GET /:bucket</code> - List objects (S3)</summary>

Supports both ListObjects V1 and V2 (`list-type=2`).

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| prefix | string | Filter by key prefix |
| delimiter | string | Hierarchy delimiter (e.g., "/") |
| max-keys | integer | Maximum objects to return |
| marker / continuation-token | string | Pagination (V1 / V2) |

**Response (200 OK):** XML ListBucketResult

**Subresources on the same path:**
- `GET /:bucket?versions` - ListObjectVersions (Version and DeleteMarker entries, keys ascending and newest first per key, `IsLatest` on each key's newest entry; supports `prefix`, `max-keys` (≤1000), `key-marker` / `version-id-marker` pagination with `IsTruncated`, `NextKeyMarker`, `NextVersionIdMarker`)
- `GET /:bucket?uploads` - ListMultipartUploads (supports `prefix`; non-admins see only their own uploads)
- `GET /:bucket?versioning` - Current versioning status
- `GET /:bucket?lifecycle` - Lifecycle configuration
- `GET /:bucket?location` - GetBucketLocation
- `GET /:bucket?policy`, `?policyStatus`, `?acl`, `?encryption`, ... - see [Bucket sub-resources](#bucket-sub-resources)
- Any other query parameter - `501 NotImplemented` (never a listing)

</details>

<details>
<summary><code>PUT /:bucket/:key</code> - Put object / CopyObject / UploadPart (S3)</summary>

**Required Headers:**
- `Content-Length`: File size
- `Content-Type`: MIME type

**Optional Headers:**
- `x-amz-meta-*`: User metadata (2KB total; exceeding it returns `MetadataTooLarge`)
- `x-amz-tagging`: Object tags, URL-query encoded (max 10 tags; key <= 128 chars, value <= 256)
- `x-amz-copy-source`: Makes this a **CopyObject**; combine with `x-amz-metadata-directive: COPY|REPLACE` and `x-amz-tagging-directive`
- `?partNumber=N&uploadId=ID` query: **UploadPart**; with `x-amz-copy-source` (plus optional `x-amz-copy-source-range: bytes=start-end`) it's **UploadPartCopy** (bad range returns `InvalidRange` 416)
- `Content-MD5`, `x-amz-checksum-crc32|crc32c|crc64nvme|sha1|sha256`: verified against the received bytes (PutObject and UploadPart), as are checksum trailers announced in `X-Amz-Trailer` for `STREAMING-UNSIGNED-PAYLOAD-TRAILER` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` bodies. A mismatch returns `400 BadDigest` and nothing is stored; a malformed value returns `400 InvalidDigest`/`InvalidRequest`.

An upload body that delivers no bytes for **60 seconds** is aborted (the key's write lock and quota reservation are released); slow but steadily progressing uploads are not time-limited. CopyObject verifies that the streamed source has exactly its recorded size.

**Response Headers:**
- `ETag`: MD5 hash of uploaded object
- `x-amz-version-id`: On versioning-enabled buckets

**Error Codes:**
- `400` - `BadDigest` (checksum mismatch), `IncompleteBody`
- `403` - `QuotaExceeded` when the write would exceed the bucket quota
- `411` - Missing Content-Length
- `413` - Entity too large

</details>

<details>
<summary><code>GET /:bucket/:key</code> - Get object (S3)</summary>

**Query Parameters:**
- `versionId` - Fetch a specific version
- `uploadId` - ListParts for a multipart upload
- `tagging` - Get object tags (AWS `<Tagging><TagSet>` XML)

Supports `Range` requests. Responses echo `x-amz-meta-*` metadata (lowercase keys), `x-amz-tagging-count`, and `x-amz-version-id` on versioned buckets.

**Response:** Binary file stream with appropriate headers

</details>

<details>
<summary><code>HEAD /:bucket/:key</code> - Head object (S3)</summary>

Supports `?versionId`.

**Response Headers:**
- `Content-Type`, `Content-Length`, `ETag`, `Last-Modified`, `x-amz-meta-*`, and `x-amz-version-id` on versioned buckets

</details>

<details>
<summary><code>DELETE /:bucket/:key</code> - Delete object (S3)</summary>

**Status:** `204 No Content`

**Query Parameters:**
- `versionId` - Permanently delete a specific version or delete marker
- `uploadId` - AbortMultipartUpload
- `tagging` - Delete object tags

On a **versioning-enabled** bucket, a plain DELETE creates a **delete marker** (`x-amz-delete-marker: true`; the object is hidden, data kept). Deleting a marker by `versionId` resurrects the object; deleting the current version by id promotes the next-newest version. Deleting a version that does not exist succeeds (`204`); a version still under WORM retention returns `403 AccessDenied`.

`POST /:bucket?delete` (DeleteObjects) honors `<VersionId>` per entry with the same authorization (`s3:DeleteObject`) and retention rules, and reports `VersionId`, `DeleteMarker` and `DeleteMarkerVersionId` in each `<Deleted>` entry like AWS.

</details>

<details>
<summary><code>PUT/GET /:bucket?versioning</code> - Bucket versioning (S3)</summary>

`PUT` accepts AWS `VersioningConfiguration` XML with `Enabled` or `Suspended` (bucket owner or admin). `GET` returns the real status.

**Deviations from AWS:**
- `Suspended` means "stop creating new versions" — history stays browsable; there is no AWS-style "null version" overwrite behavior.
- Version IDs are UUIDs; objects written before versioning report version id `null`.
- Console move/rename relocate an object with its identity — no versions/markers are recorded.
- Version bytes live in hidden storage (local backend: outside bucket directories; S3 backend: a `.bkt-versions/` prefix inside the real bucket, excluded from listings).

</details>

<details>
<summary><code>PUT/GET/DELETE /:bucket?lifecycle</code> - Bucket lifecycle (S3)</summary>

Single rule per bucket (a multi-rule configuration returns `NotImplemented`). Supported AWS XML subset:
- `Expiration.Days` - expire current objects (via the versioned path this creates delete markers)
- `Filter.Prefix` - limit the rule to a key prefix
- `NoncurrentVersionExpiration.NoncurrentDays` - permanently remove noncurrent versions

Rules are applied hourly, plus once ~30s after startup.

</details>

<details>
<summary><code>PUT /:bucket</code> - Create bucket (S3) - DISABLED</summary>

**Status:** `403 Forbidden`

Bucket creation via S3 API is disabled. Use the web UI or REST API. (`PUT /:bucket?versioning` and `PUT /:bucket?lifecycle` on existing buckets still work.)

</details>

---

## Error Handling

### Standard Error Response

```json
{
  "error": "Error type",
  "message": "Detailed error message"
}
```

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | OK - Request successful |
| 201 | Created - Resource created |
| 202 | Accepted - Async operation started |
| 204 | No Content - Success with no body |
| 400 | Bad Request - Validation error |
| 401 | Unauthorized - Missing or invalid token |
| 403 | Forbidden - Permission denied |
| 404 | Not Found - Resource doesn't exist |
| 409 | Conflict - Duplicate or resource in use |
| 411 | Length Required - Missing Content-Length |
| 413 | Payload Too Large - File exceeds limit |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error |

---

## Security Features

### Authentication
- JWT tokens with configurable expiration
- Refresh token rotation with reuse detection (replaying a rotated token revokes all of the user's sessions)
- Logout revokes both the access token and its refresh token
- Account locking capability
- SSO support (Google OAuth, Vault JWT, generic OIDC via `VAULT_OIDC_*`); SSO logins are audited

### Authorization
- Role-based access (admin/user)
- Policy-based bucket and object permissions
- Per-resource access control
- Automatic policy sync from SSO JWT claims (Vault)

### Data Protection
- Passwords hashed with bcrypt
- S3 credentials encrypted at rest
- Secret keys shown only once at creation
- Constant-time comparison for sensitive values

### Input Validation
- Content type detection from file magic numbers
- Path traversal prevention
- SQL injection protection
- Rate limiting on authentication endpoints

### Headers
- Idempotency support via `Idempotency-Key` header
- Cache control for sensitive responses
- CORS configuration support

---

## Related Documentation

- [SSO Setup Guide](../guides/sso-setup.md) - Configure Vault JWT and Google OAuth SSO
- [Policies API](policies.md) - Detailed policy management documentation
- [Access Keys API](access-keys.md) - S3-compatible access key management
- [Security Overview](../security/security-overview.md) - Comprehensive security architecture
- [Production Checklist](../deployment/production-checklist.md) - Deployment preparation
