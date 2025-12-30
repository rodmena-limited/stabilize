from stabilize.config_validation import (
    SHELL_TASK_SCHEMA,
    WAIT_TASK_SCHEMA,
    SchemaValidator,
    ValidationError,
    is_valid,
    validate_context,
    validate_outputs,
)

class TestValidationError:
    """Tests for ValidationError class."""

    def test_str_with_path(self) -> None:
        """Test string representation with path."""
        error = ValidationError("timeout", "must be >= 0")
        assert str(error) == "timeout: must be >= 0"

    def test_str_without_path(self) -> None:
        """Test string representation without path."""
        error = ValidationError("", "missing required field")
        assert str(error) == "missing required field"

    def test_attributes(self) -> None:
        """Test error attributes."""
        error = ValidationError(
            path="config.timeout",
            message="must be positive",
            value=-1,
            constraint="minimum",
        )
        assert error.path == "config.timeout"
        assert error.message == "must be positive"
        assert error.value == -1
        assert error.constraint == "minimum"

class TestSchemaValidatorTypes:
    """Tests for type validation."""

    def test_string_type(self) -> None:
        """Test string type validation."""
        validator = SchemaValidator({"type": "string"})
        assert validator.validate("hello") == []
        errors = validator.validate(123)
        assert len(errors) == 1
        assert "string" in errors[0].message

    def test_integer_type(self) -> None:
        """Test integer type validation."""
        validator = SchemaValidator({"type": "integer"})
        assert validator.validate(42) == []
        errors = validator.validate("42")
        assert len(errors) == 1

    def test_integer_rejects_boolean(self) -> None:
        """Test that integer type rejects boolean."""
        validator = SchemaValidator({"type": "integer"})
        errors = validator.validate(True)
        assert len(errors) == 1

    def test_number_type(self) -> None:
        """Test number type validation (int or float)."""
        validator = SchemaValidator({"type": "number"})
        assert validator.validate(42) == []
        assert validator.validate(3.14) == []
        errors = validator.validate("42")
        assert len(errors) == 1

    def test_boolean_type(self) -> None:
        """Test boolean type validation."""
        validator = SchemaValidator({"type": "boolean"})
        assert validator.validate(True) == []
        assert validator.validate(False) == []
        errors = validator.validate(1)
        assert len(errors) == 1

    def test_array_type(self) -> None:
        """Test array type validation."""
        validator = SchemaValidator({"type": "array"})
        assert validator.validate([1, 2, 3]) == []
        errors = validator.validate("not an array")
        assert len(errors) == 1

    def test_object_type(self) -> None:
        """Test object type validation."""
        validator = SchemaValidator({"type": "object"})
        assert validator.validate({"key": "value"}) == []
        errors = validator.validate([])
        assert len(errors) == 1

    def test_null_type(self) -> None:
        """Test null type validation."""
        validator = SchemaValidator({"type": "null"})
        assert validator.validate(None) == []
        errors = validator.validate("")
        assert len(errors) == 1

    def test_union_type(self) -> None:
        """Test union type validation."""
        validator = SchemaValidator({"type": ["string", "integer"]})
        assert validator.validate("hello") == []
        assert validator.validate(42) == []
        errors = validator.validate(3.14)
        assert len(errors) == 1

class TestSchemaValidatorString:
    """Tests for string validation."""

    def test_min_length(self) -> None:
        """Test minimum length validation."""
        validator = SchemaValidator({"type": "string", "minLength": 3})
        assert validator.validate("hello") == []
        errors = validator.validate("hi")
        assert len(errors) == 1
        assert "minimum length" in errors[0].message

    def test_max_length(self) -> None:
        """Test maximum length validation."""
        validator = SchemaValidator({"type": "string", "maxLength": 5})
        assert validator.validate("hello") == []
        errors = validator.validate("hello world")
        assert len(errors) == 1
        assert "maximum length" in errors[0].message

    def test_pattern(self) -> None:
        """Test pattern validation."""
        validator = SchemaValidator({"type": "string", "pattern": r"^\d{3}-\d{4}$"})
        assert validator.validate("123-4567") == []
        errors = validator.validate("12-345")
        assert len(errors) == 1
        assert "pattern" in errors[0].message
