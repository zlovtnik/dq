package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// CanonicalJSON matches the control API's recursively sorted, compact UTF-8
// representation with literal HTML characters and preserved JSON numbers.
func CanonicalJSON(data []byte) ([]byte, error) {
	if len(data) > MaxRecordBytes || !utf8.Valid(data) {
		return nil, errors.New("invalid bounded UTF-8 control document")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeCanonical(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}
func decodeCanonical(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				name, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := name.(string)
				if !ok {
					return nil, errors.New("invalid JSON name")
				}
				if _, exists := object[key]; exists {
					return nil, errors.New("duplicate JSON name")
				}
				value, err := decodeCanonical(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errors.New("invalid JSON object")
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				value, err := decodeCanonical(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errors.New("invalid JSON array")
			}
			return array, nil
		default:
			return nil, errors.New("invalid JSON delimiter")
		}
	}
	return token, nil
}
func ManifestDigest(manifest Manifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(data, &object); err != nil {
		return "", err
	}
	delete(object, "manifest_sha256")
	data, err = json.Marshal(object)
	if err != nil {
		return "", err
	}
	canonical, err := CanonicalJSON(data)
	if err != nil {
		return "", err
	}
	return SHA256(canonical), nil
}
