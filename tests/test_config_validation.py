"""Tests for the configuration validation module."""

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


class TestSchemaValidatorNumber:
    """Tests for number validation."""

    def test_minimum(self) -> None:
        """Test minimum value validation."""
        validator = SchemaValidator({"type": "integer", "minimum": 0})
        assert validator.validate(0) == []
        assert validator.validate(10) == []
        errors = validator.validate(-1)
        assert len(errors) == 1
        assert ">=" in errors[0].message

    def test_maximum(self) -> None:
        """Test maximum value validation."""
        validator = SchemaValidator({"type": "integer", "maximum": 100})
        assert validator.validate(100) == []
        errors = validator.validate(101)
        assert len(errors) == 1
        assert "<=" in errors[0].message

    def test_exclusive_minimum(self) -> None:
        """Test exclusive minimum validation."""
        validator = SchemaValidator({"type": "integer", "exclusiveMinimum": 0})
        assert validator.validate(1) == []
        errors = validator.validate(0)
        assert len(errors) == 1
        assert ">" in errors[0].message

    def test_exclusive_maximum(self) -> None:
        """Test exclusive maximum validation."""
        validator = SchemaValidator({"type": "integer", "exclusiveMaximum": 100})
        assert validator.validate(99) == []
        errors = validator.validate(100)
        assert len(errors) == 1
        assert "<" in errors[0].message

    def test_multiple_of(self) -> None:
        """Test multiple of validation."""
        validator = SchemaValidator({"type": "integer", "multipleOf": 5})
        assert validator.validate(10) == []
        errors = validator.validate(7)
        assert len(errors) == 1
        assert "multiple" in errors[0].message


class TestSchemaValidatorArray:
    """Tests for array validation."""

    def test_min_items(self) -> None:
        """Test minimum items validation."""
        validator = SchemaValidator({"type": "array", "minItems": 2})
        assert validator.validate([1, 2]) == []
        errors = validator.validate([1])
        assert len(errors) == 1
        assert "at least" in errors[0].message

    def test_max_items(self) -> None:
        """Test maximum items validation."""
        validator = SchemaValidator({"type": "array", "maxItems": 3})
        assert validator.validate([1, 2, 3]) == []
        errors = validator.validate([1, 2, 3, 4])
        assert len(errors) == 1
        assert "at most" in errors[0].message

    def test_unique_items(self) -> None:
        """Test unique items validation."""
        validator = SchemaValidator({"type": "array", "uniqueItems": True})
        assert validator.validate([1, 2, 3]) == []
        errors = validator.validate([1, 2, 1])
        assert len(errors) == 1
        assert "unique" in errors[0].message

    def test_items_schema(self) -> None:
        """Test items schema validation."""
        validator = SchemaValidator(
            {
                "type": "array",
                "items": {"type": "integer", "minimum": 0},
            }
        )
        assert validator.validate([1, 2, 3]) == []
        errors = validator.validate([1, -1, 3])
        assert len(errors) == 1
        assert "[1]" in errors[0].path


class TestSchemaValidatorObject:
    """Tests for object validation."""

    def test_required_present(self) -> None:
        """Test required field validation - present."""
        validator = SchemaValidator(
            {
                "type": "object",
                "required": ["name", "age"],
            }
        )
        assert validator.validate({"name": "John", "age": 30}) == []

    def test_required_missing(self) -> None:
        """Test required field validation - missing."""
        validator = SchemaValidator(
            {
                "type": "object",
                "required": ["name", "age"],
            }
        )
        errors = validator.validate({"name": "John"})
        assert len(errors) == 1
        assert "age" in errors[0].path
        assert "required" in errors[0].message

    def test_properties(self) -> None:
        """Test property schema validation."""
        validator = SchemaValidator(
            {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer", "minimum": 0},
                },
            }
        )
        assert validator.validate({"name": "John", "age": 30}) == []
        errors = validator.validate({"name": "John", "age": -1})
        assert len(errors) == 1
        assert "age" in errors[0].path

    def test_additional_properties_false(self) -> None:
        """Test additionalProperties: false."""
        validator = SchemaValidator(
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "additionalProperties": False,
            }
        )
        assert validator.validate({"name": "John"}) == []
        errors = validator.validate({"name": "John", "extra": "field"})
        assert len(errors) == 1
        assert "extra" in errors[0].path

    def test_additional_properties_schema(self) -> None:
        """Test additionalProperties with schema."""
        validator = SchemaValidator(
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "additionalProperties": {"type": "integer"},
            }
        )
        assert validator.validate({"name": "John", "age": 30}) == []
        errors = validator.validate({"name": "John", "age": "thirty"})
        assert len(errors) == 1

    def test_min_properties(self) -> None:
        """Test minimum properties validation."""
        validator = SchemaValidator(
            {
                "type": "object",
                "minProperties": 2,
            }
        )
        assert validator.validate({"a": 1, "b": 2}) == []
        errors = validator.validate({"a": 1})
        assert len(errors) == 1

    def test_max_properties(self) -> None:
        """Test maximum properties validation."""
        validator = SchemaValidator(
            {
                "type": "object",
                "maxProperties": 2,
            }
        )
        assert validator.validate({"a": 1, "b": 2}) == []
        errors = validator.validate({"a": 1, "b": 2, "c": 3})
        assert len(errors) == 1


class TestSchemaValidatorEnum:
    """Tests for enum validation."""

    def test_enum_valid(self) -> None:
        """Test valid enum value."""
        validator = SchemaValidator({"enum": ["red", "green", "blue"]})
        assert validator.validate("red") == []

    def test_enum_invalid(self) -> None:
        """Test invalid enum value."""
        validator = SchemaValidator({"enum": ["red", "green", "blue"]})
        errors = validator.validate("yellow")
        assert len(errors) == 1
        assert "one of" in errors[0].message


class TestSchemaValidatorConst:
    """Tests for const validation."""

    def test_const_match(self) -> None:
        """Test matching const value."""
        validator = SchemaValidator({"const": "fixed"})
        assert validator.validate("fixed") == []

    def test_const_no_match(self) -> None:
        """Test non-matching const value."""
        validator = SchemaValidator({"const": "fixed"})
        errors = validator.validate("different")
        assert len(errors) == 1
        assert "exactly" in errors[0].message


class TestValidateContext:
    """Tests for validate_context function."""

    def test_valid_context(self) -> None:
        """Test validating a valid context."""
        schema = {
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string", "minLength": 1},
            },
        }
        errors = validate_context({"command": "ls -la"}, schema)
        assert errors == []

    def test_invalid_context(self) -> None:
        """Test validating an invalid context."""
        schema = {
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string", "minLength": 1},
            },
        }
        errors = validate_context({}, schema)
        assert len(errors) == 1


class TestValidateOutputs:
    """Tests for validate_outputs function."""

    def test_valid_outputs(self) -> None:
        """Test validating valid outputs."""
        schema = {
            "type": "object",
            "required": ["stdout", "returncode"],
            "properties": {
                "stdout": {"type": "string"},
                "returncode": {"type": "integer"},
            },
        }
        errors = validate_outputs({"stdout": "hello", "returncode": 0}, schema)
        assert errors == []


class TestIsValid:
    """Tests for is_valid function."""

    def test_is_valid_true(self) -> None:
        """Test is_valid returns True for valid data."""
        assert is_valid(42, {"type": "integer"})

    def test_is_valid_false(self) -> None:
        """Test is_valid returns False for invalid data."""
        assert not is_valid("hello", {"type": "integer"})


class TestBuiltInSchemas:
    """Tests for built-in schemas."""

    def test_shell_task_schema_valid(self) -> None:
        """Test SHELL_TASK_SCHEMA with valid data."""
        context = {
            "command": "echo hello",
            "timeout": 60,
            "cwd": "/home",
        }
        errors = validate_context(context, SHELL_TASK_SCHEMA)
        assert errors == []

    def test_shell_task_schema_missing_command(self) -> None:
        """Test SHELL_TASK_SCHEMA with missing command."""
        context = {"timeout": 60}
        errors = validate_context(context, SHELL_TASK_SCHEMA)
        assert len(errors) == 1
        assert "command" in errors[0].path

    def test_wait_task_schema_valid(self) -> None:
        """Test WAIT_TASK_SCHEMA with valid data."""
        context = {"waitTime": 30}
        errors = validate_context(context, WAIT_TASK_SCHEMA)
        assert errors == []

    def test_wait_task_schema_invalid(self) -> None:
        """Test WAIT_TASK_SCHEMA with invalid data."""
        context = {"waitTime": -5}
        errors = validate_context(context, WAIT_TASK_SCHEMA)
        assert len(errors) == 1


class TestNestedValidation:
    """Tests for nested structure validation."""

    def test_nested_object(self) -> None:
        """Test validation of nested objects."""
        schema = {
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "required": ["host"],
                    "properties": {
                        "host": {"type": "string"},
                        "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    },
                },
            },
        }
        assert validate_context({"config": {"host": "localhost", "port": 8080}}, schema) == []
        errors = validate_context({"config": {"port": 8080}}, schema)
        assert len(errors) == 1
        assert "config.host" in errors[0].path

    def test_array_of_objects(self) -> None:
        """Test validation of array of objects."""
        schema = {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                },
            },
        }
        assert validate_context([{"name": "item1"}, {"name": "item2"}], schema) == []
        errors = validate_context([{"name": "item1"}, {}], schema)
        assert len(errors) == 1
        assert "[1].name" in errors[0].path
