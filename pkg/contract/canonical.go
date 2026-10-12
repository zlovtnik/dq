package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// CanonicalJSON matches the control API's recursively sorted, compact UTF-8
// representation with literal HTML characters and exact plain decimal numbers.
// Numeric spellings normalize before hashing so JSONB round trips preserve
// identity without losing precision through floating point conversion.
func CanonicalJSON(data []byte) ([]byte, error) {
	if len(data) > MaxRecordBytes || !utf8.Valid(data) {
		return nil, errors.New("invalid bounded UTF-8 control document")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	budget := MaxRecordBytes
	value, err := decodeCanonical(decoder, 0, &budget)
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
	canonical := bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
	if len(canonical) > MaxRecordBytes {
		return nil, errors.New("canonical document exceeds limit")
	}
	return canonical, nil
}
func decodeCanonical(decoder *json.Decoder, depth int, budget *int) (any, error) {
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
				value, err := decodeCanonical(decoder, depth+1, budget)
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
				value, err := decodeCanonical(decoder, depth+1, budget)
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
	if number, ok := token.(json.Number); ok {
		normalized, err := normalizeNumber(number, *budget)
		if err != nil {
			return nil, err
		}
		*budget -= len(normalized)
		return normalized, nil
	}
	return token, nil
}

func normalizeNumber(number json.Number, budget int) (json.Number, error) {
	text := string(number)
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	exponent := 0
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		parsed, err := strconv.ParseInt(text[index+1:], 10, 64)
		if err != nil || parsed < -MaxRecordBytes || parsed > MaxRecordBytes {
			return "", errors.New("JSON number expansion exceeds limit")
		}
		exponent = int(parsed)
		text = text[:index]
	}
	if point := strings.IndexByte(text, '.'); point >= 0 {
		exponent -= len(text) - point - 1
		text = text[:point] + text[point+1:]
	}
	text = strings.TrimLeft(text, "0")
	if text == "" {
		if budget < 1 {
			return "", errors.New("JSON number expansion exceeds limit")
		}
		return "0", nil
	}
	trimmed := strings.TrimRight(text, "0")
	exponent += len(text) - len(trimmed)
	text = trimmed
	sign := ""
	if negative {
		sign = "-"
	}
	point := len(text) + exponent
	length := len(sign) + len(text)
	switch {
	case exponent >= 0:
		length += exponent
	case point <= 0:
		length += 2 - point
	default:
		length++
	}
	if length > budget {
		return "", errors.New("JSON number expansion exceeds limit")
	}
	switch {
	case exponent >= 0:
		return json.Number(sign + text + strings.Repeat("0", exponent)), nil
	case point <= 0:
		return json.Number(sign + "0." + strings.Repeat("0", -point) + text), nil
	default:
		return json.Number(sign + text[:point] + "." + text[point:]), nil
	}
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
