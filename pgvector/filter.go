package pgvector

import (
	"fmt"
	"strings"

	"github.com/zoobzio/grub"
	"github.com/zoobzio/vecna"
)

// translateFilter converts a vecna.Filter to SQL WHERE clause with positional params.
// Returns the WHERE clause, args slice, and next parameter index.
func translateFilter(f *vecna.Filter, metadataColumn string, startIdx int) (string, []any, int, error) {
	if f == nil {
		return "", nil, startIdx, nil
	}

	if err := f.Err(); err != nil {
		return "", nil, startIdx, fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	var args []any
	clause, nextIdx, err := translateNode(f, metadataColumn, startIdx, &args)
	if err != nil {
		return "", nil, startIdx, err
	}

	return clause, args, nextIdx, nil
}

// translateNode recursively translates a filter node.
func translateNode(f *vecna.Filter, metadataColumn string, paramIdx int, args *[]any) (string, int, error) {
	switch f.Op() {
	case vecna.And:
		return translateAnd(f.Children(), metadataColumn, paramIdx, args)
	case vecna.Or:
		return translateOr(f.Children(), metadataColumn, paramIdx, args)
	case vecna.Not:
		return translateNot(f.Children(), metadataColumn, paramIdx, args)
	default:
		return translateFieldCondition(f, metadataColumn, paramIdx, args)
	}
}

// translateAnd translates an AND filter.
func translateAnd(children []*vecna.Filter, metadataColumn string, paramIdx int, args *[]any) (string, int, error) {
	if len(children) == 0 {
		return "", paramIdx, nil
	}

	var parts []string
	for _, child := range children {
		part, nextIdx, err := translateNode(child, metadataColumn, paramIdx, args)
		if err != nil {
			return "", paramIdx, err
		}
		if part != "" {
			parts = append(parts, part)
		}
		paramIdx = nextIdx
	}

	if len(parts) == 0 {
		return "", paramIdx, nil
	}
	if len(parts) == 1 {
		return parts[0], paramIdx, nil
	}
	return "(" + strings.Join(parts, " AND ") + ")", paramIdx, nil
}

// translateOr translates an OR filter.
func translateOr(children []*vecna.Filter, metadataColumn string, paramIdx int, args *[]any) (string, int, error) {
	if len(children) == 0 {
		return "", paramIdx, nil
	}

	var parts []string
	for _, child := range children {
		part, nextIdx, err := translateNode(child, metadataColumn, paramIdx, args)
		if err != nil {
			return "", paramIdx, err
		}
		if part != "" {
			parts = append(parts, part)
		}
		paramIdx = nextIdx
	}

	if len(parts) == 0 {
		return "", paramIdx, nil
	}
	if len(parts) == 1 {
		return parts[0], paramIdx, nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", paramIdx, nil
}

// translateNot translates a NOT filter.
func translateNot(children []*vecna.Filter, metadataColumn string, paramIdx int, args *[]any) (string, int, error) {
	if len(children) != 1 {
		return "", paramIdx, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}

	part, nextIdx, err := translateNode(children[0], metadataColumn, paramIdx, args)
	if err != nil {
		return "", paramIdx, err
	}
	if part == "" {
		return "", nextIdx, nil
	}

	return "NOT " + part, nextIdx, nil
}

// translateFieldCondition translates a field condition.
func translateFieldCondition(f *vecna.Filter, metadataColumn string, paramIdx int, args *[]any) (string, int, error) {
	field := f.Field()
	value := f.Value()

	// Build JSONB field access: metadata->>'field'
	jsonbField := fmt.Sprintf(`%q->>'%s'`, metadataColumn, field)

	switch f.Op() {
	case vecna.Eq:
		return translateComparison(jsonbField, "=", value, paramIdx, args)
	case vecna.Ne:
		return translateComparison(jsonbField, "!=", value, paramIdx, args)
	case vecna.Gt:
		return translateNumericComparison(jsonbField, ">", value, paramIdx, args)
	case vecna.Gte:
		return translateNumericComparison(jsonbField, ">=", value, paramIdx, args)
	case vecna.Lt:
		return translateNumericComparison(jsonbField, "<", value, paramIdx, args)
	case vecna.Lte:
		return translateNumericComparison(jsonbField, "<=", value, paramIdx, args)
	case vecna.In:
		return translateIn(jsonbField, value, paramIdx, args)
	case vecna.Nin:
		return translateNin(jsonbField, value, paramIdx, args)
	case vecna.Like:
		return translateLike(jsonbField, value, paramIdx, args)
	case vecna.Contains:
		return translateContains(metadataColumn, field, value, paramIdx, args)
	default:
		return "", paramIdx, fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// translateComparison translates a simple comparison.
func translateComparison(field, op string, value any, paramIdx int, args *[]any) (string, int, error) {
	*args = append(*args, fmt.Sprintf("%v", value))
	clause := fmt.Sprintf("%s %s $%d", field, op, paramIdx)
	return clause, paramIdx + 1, nil
}

// translateNumericComparison translates a numeric comparison with cast.
func translateNumericComparison(field, op string, value any, paramIdx int, args *[]any) (string, int, error) {
	// Cast JSONB text to numeric for comparison.
	switch v := value.(type) {
	case int, int32, int64, float32, float64:
		*args = append(*args, v)
		clause := fmt.Sprintf("(%s)::numeric %s $%d", field, op, paramIdx)
		return clause, paramIdx + 1, nil
	default:
		return "", paramIdx, fmt.Errorf("%w: numeric comparison requires numeric value, got %T", grub.ErrInvalidQuery, value)
	}
}

// translateIn translates an IN condition.
func translateIn(field string, value any, paramIdx int, args *[]any) (string, int, error) {
	slice, ok := value.([]any)
	if !ok {
		return "", paramIdx, fmt.Errorf("%w: In requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// IN empty set is always false.
		return "FALSE", paramIdx, nil
	}

	// Convert all values to strings for JSONB text comparison.
	strValues := make([]string, len(slice))
	for i, v := range slice {
		strValues[i] = fmt.Sprintf("%v", v)
	}

	// Use PostgreSQL array syntax.
	arrayStr := stringArrayToPgArray(strValues)
	*args = append(*args, arrayStr)
	clause := fmt.Sprintf("%s = ANY($%d::text[])", field, paramIdx)
	return clause, paramIdx + 1, nil
}

// translateNin translates a NOT IN condition.
func translateNin(field string, value any, paramIdx int, args *[]any) (string, int, error) {
	slice, ok := value.([]any)
	if !ok {
		return "", paramIdx, fmt.Errorf("%w: Nin requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// NOT IN empty set is always true.
		return "TRUE", paramIdx, nil
	}

	// Convert all values to strings for JSONB text comparison.
	strValues := make([]string, len(slice))
	for i, v := range slice {
		strValues[i] = fmt.Sprintf("%v", v)
	}

	// Use PostgreSQL array syntax with ALL for NOT IN.
	arrayStr := stringArrayToPgArray(strValues)
	*args = append(*args, arrayStr)
	clause := fmt.Sprintf("%s != ALL($%d::text[])", field, paramIdx)
	return clause, paramIdx + 1, nil
}

// translateLike translates a LIKE condition.
func translateLike(field string, value any, paramIdx int, args *[]any) (string, int, error) {
	pattern, ok := value.(string)
	if !ok {
		return "", paramIdx, fmt.Errorf("%w: Like requires string pattern", grub.ErrInvalidQuery)
	}

	*args = append(*args, pattern)
	clause := fmt.Sprintf("%s LIKE $%d", field, paramIdx)
	return clause, paramIdx + 1, nil
}

// translateContains translates an array contains condition.
func translateContains(metadataColumn, field string, value any, paramIdx int, args *[]any) (string, int, error) {
	// For JSONB array contains, use @> operator with JSONB value.
	// metadata->'field' @> '["value"]'::jsonb
	var jsonArray string
	switch v := value.(type) {
	case string:
		jsonArray = fmt.Sprintf(`["%s"]`, v)
	default:
		jsonArray = fmt.Sprintf(`[%v]`, v)
	}

	*args = append(*args, jsonArray)
	clause := fmt.Sprintf(`%q->'%s' @> $%d::jsonb`, metadataColumn, field, paramIdx)
	return clause, paramIdx + 1, nil
}
