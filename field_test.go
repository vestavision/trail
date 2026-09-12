package trail

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestTypedFields(t *testing.T) {
	tests := []struct {
		option Option
		kind   FieldKind
		check  func(Field) bool
	}{
		{String("s", "value"), FieldString, func(f Field) bool { return f.Text() == "value" }},
		{Int("i", -3), FieldInt, func(f Field) bool { return f.Int64() == -3 }},
		{Int64("i64", 99), FieldInt64, func(f Field) bool { return f.Int64() == 99 }},
		{Bool("b", true), FieldBool, func(f Field) bool { return f.Bool() }},
		{Float64("f", math.Pi), FieldFloat64, func(f Field) bool { return f.Float64() == math.Pi }},
		{Duration("d", 2*time.Second), FieldDuration, func(f Field) bool { return f.Duration() == 2*time.Second }},
		{Error(errors.New("boom")), FieldError, func(f Field) bool { return f.Key() == "error" && f.Text() == "boom" }},
		{Error(nil), FieldError, func(f Field) bool { return f.Text() == "<nil>" }},
	}
	for _, test := range tests {
		if test.option.field.Kind() != test.kind || !test.check(test.option.field) {
			t.Errorf("field %q did not retain its typed value", test.option.field.Key())
		}
	}
}
