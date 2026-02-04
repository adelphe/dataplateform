"""Tests for the schema detection module."""

import json
import os
import tempfile

import pytest

from ingestion.schema_detector import (
    SchemaDetectionError,
    detect_csv_schema,
    detect_json_schema,
)


class TestDetectCsvSchema:
    """Tests for detect_csv_schema."""

    def test_detects_columns(self, sample_csv_file):
        schema = detect_csv_schema(sample_csv_file)

        assert schema["format"] == "csv"
        assert len(schema["columns"]) == 3
        col_names = [c["name"] for c in schema["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names

    def test_detects_types(self, sample_csv_file):
        schema = detect_csv_schema(sample_csv_file)

        id_col = next(c for c in schema["columns"] if c["name"] == "id")
        assert id_col["type"] == "integer"

        name_col = next(c for c in schema["columns"] if c["name"] == "name")
        assert name_col["type"] == "string"

    def test_detects_row_count(self, sample_csv_file):
        schema = detect_csv_schema(sample_csv_file)
        assert schema["row_count"] == 2

    def test_nullable_detection(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            f.write("a,b\n1,x\n2,\n")
            path = f.name
        try:
            schema = detect_csv_schema(path)
            b_col = next(c for c in schema["columns"] if c["name"] == "b")
            assert b_col["nullable"] is True
        finally:
            os.unlink(path)

    def test_missing_file_raises(self):
        with pytest.raises(SchemaDetectionError, match="Failed to read CSV"):
            detect_csv_schema("/nonexistent/path.csv")


class TestDetectJsonSchema:
    """Tests for detect_json_schema."""

    def test_detects_columns(self, sample_json_file):
        schema = detect_json_schema(sample_json_file)

        assert schema["format"] == "json"
        col_names = [c["name"] for c in schema["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
        assert "timestamp" in col_names

    def test_detects_types(self, sample_json_file):
        schema = detect_json_schema(sample_json_file)

        id_col = next(c for c in schema["columns"] if c["name"] == "id")
        assert id_col["type"] == "integer"

        amount_col = next(c for c in schema["columns"] if c["name"] == "amount")
        assert amount_col["type"] == "float"

    def test_detects_row_count(self, sample_json_file):
        schema = detect_json_schema(sample_json_file)
        assert schema["row_count"] == 2

    def test_single_object_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"key": "value", "num": 42}, f)
            path = f.name
        try:
            schema = detect_json_schema(path)
            assert schema["row_count"] == 1
            assert len(schema["columns"]) == 2
        finally:
            os.unlink(path)

    def test_empty_array_raises(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump([], f)
            path = f.name
        try:
            with pytest.raises(SchemaDetectionError, match="no records"):
                detect_json_schema(path)
        finally:
            os.unlink(path)

    def test_missing_file_raises(self):
        with pytest.raises(SchemaDetectionError, match="Failed to read JSON"):
            detect_json_schema("/nonexistent/path.json")

    def test_ndjson_format(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            f.write('{"a": 1}\n{"a": 2}\n')
            path = f.name
        try:
            # First attempt reads as JSON array (will fail), then tries NDJSON
            schema = detect_json_schema(path)
            assert schema["row_count"] == 2
        finally:
            os.unlink(path)
