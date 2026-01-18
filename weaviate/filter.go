package weaviate

import (
	"fmt"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/zoobzio/grub"
	"github.com/zoobzio/vecna"
)

// translateFilter converts a vecna.Filter to a Weaviate WhereBuilder.
func translateFilter(f *vecna.Filter) (*filters.WhereBuilder, error) {
	if f == nil {
		return nil, nil
	}

	if err := f.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	return translateNode(f)
}

// translateNode recursively translates a filter node.
func translateNode(f *vecna.Filter) (*filters.WhereBuilder, error) {
	switch f.Op() {
	case vecna.And:
		return translateLogical(f.Children(), filters.And)
	case vecna.Or:
		return translateLogical(f.Children(), filters.Or)
	case vecna.Not:
		return translateNot(f.Children())
	default:
		return translateCondition(f)
	}
}

// translateLogical translates AND/OR filters.
func translateLogical(children []*vecna.Filter, op filters.WhereOperator) (*filters.WhereBuilder, error) {
	if len(children) == 0 {
		return nil, nil
	}

	clauses := make([]*filters.WhereBuilder, 0, len(children))
	for _, child := range children {
		clause, err := translateNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return filters.Where().
		WithOperator(op).
		WithOperands(clauses), nil
}

// translateNot translates a NOT filter.
func translateNot(children []*vecna.Filter) (*filters.WhereBuilder, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}

	child, err := translateNode(children[0])
	if err != nil {
		return nil, err
	}

	// Weaviate doesn't have a direct NOT operator, so we wrap with a Not operand
	return filters.Where().
		WithOperator(filters.Not).
		WithOperands([]*filters.WhereBuilder{child}), nil
}

// translateCondition translates a field condition.
func translateCondition(f *vecna.Filter) (*filters.WhereBuilder, error) {
	field := f.Field()
	value := f.Value()

	clause := filters.Where().WithPath([]string{field})

	switch f.Op() {
	case vecna.Eq:
		return applyValue(clause, filters.Equal, value)
	case vecna.Ne:
		return applyValue(clause, filters.NotEqual, value)
	case vecna.Gt:
		return applyValue(clause, filters.GreaterThan, value)
	case vecna.Gte:
		return applyValue(clause, filters.GreaterThanEqual, value)
	case vecna.Lt:
		return applyValue(clause, filters.LessThan, value)
	case vecna.Lte:
		return applyValue(clause, filters.LessThanEqual, value)
	case vecna.In:
		return translateIn(field, value)
	case vecna.Nin:
		// Nin is implemented as NOT(IN)
		inClause, err := translateIn(field, value)
		if err != nil {
			return nil, err
		}
		return filters.Where().
			WithOperator(filters.Not).
			WithOperands([]*filters.WhereBuilder{inClause}), nil
	case vecna.Like:
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: Like requires string pattern", grub.ErrInvalidQuery)
		}
		return clause.WithOperator(filters.Like).WithValueText(pattern), nil
	case vecna.Contains:
		return translateContains(field, value)
	default:
		return nil, fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// applyValue applies a value to a where clause based on its type.
func applyValue(clause *filters.WhereBuilder, op filters.WhereOperator, value any) (*filters.WhereBuilder, error) {
	clause = clause.WithOperator(op)

	switch v := value.(type) {
	case string:
		return clause.WithValueText(v), nil
	case int:
		return clause.WithValueInt(int64(v)), nil
	case int64:
		return clause.WithValueInt(v), nil
	case float64:
		return clause.WithValueNumber(v), nil
	case bool:
		return clause.WithValueBoolean(v), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T", grub.ErrInvalidQuery, value)
	}
}

// translateIn translates an IN condition using ContainsAny.
func translateIn(field string, value any) (*filters.WhereBuilder, error) {
	slice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: In requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// Empty IN matches nothing - return a filter that will always be false
		return filters.Where().WithPath([]string{field}).WithOperator(filters.Equal).WithValueText("__impossible_value__"), nil
	}

	// Convert slice to appropriate type
	clause := filters.Where().WithPath([]string{field})

	// Check first element type
	switch slice[0].(type) {
	case string:
		strings := make([]string, len(slice))
		for i, v := range slice {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
			strings[i] = s
		}
		return clause.WithOperator(filters.ContainsAny).WithValueText(strings...), nil
	case int, int64:
		ints := make([]int64, len(slice))
		for i, v := range slice {
			switch val := v.(type) {
			case int:
				ints[i] = int64(val)
			case int64:
				ints[i] = val
			default:
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
		}
		return clause.WithOperator(filters.ContainsAny).WithValueInt(ints...), nil
	case float64:
		floats := make([]float64, len(slice))
		for i, v := range slice {
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
			floats[i] = f
		}
		return clause.WithOperator(filters.ContainsAny).WithValueNumber(floats...), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type for In", grub.ErrInvalidQuery)
	}
}

// translateContains translates a Contains condition.
func translateContains(field string, value any) (*filters.WhereBuilder, error) {
	clause := filters.Where().WithPath([]string{field}).WithOperator(filters.ContainsAny)

	switch v := value.(type) {
	case string:
		return clause.WithValueText(v), nil
	case int:
		return clause.WithValueInt(int64(v)), nil
	case int64:
		return clause.WithValueInt(v), nil
	case float64:
		return clause.WithValueNumber(v), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T for Contains", grub.ErrInvalidQuery, value)
	}
}
