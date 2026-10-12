// Package schemas exposes the pinned processor JSON Schema documents.
package schemas

import "embed"

// Files contains the v1 parameter, snapshot, and proposal schemas.
//
//go:embed *.json
var Files embed.FS
