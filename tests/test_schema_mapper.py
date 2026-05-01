#!/usr/bin/env python3
"""
Unit tests for schema/mapper.py

Covers:
  - resolve_path: JSON-path resolution against dicts
  - apply_transform: all supported transforms
  - resolve_column: string and dict column definitions
  - SqlOperation: SQL generation for DELETE, INSERT, UPSERT
  - SchemaMapper: source matching, document mapping, from_file
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import date
from decimal import Decimal

# Ensure the module under test is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema.mapper import (
    SchemaMapper,
    SqlOperation,
    apply_transform,
    resolve_column,
    resolve_path,
)

# Path to the orders.json mapping fixture
ORDERS_MAPPING_PATH = os.path.join(
    os.path.dirname(__file__), "..", "mappings", "orders.json"
)


# ===================================================================
# resolve_path
# ===================================================================


class TestResolvePath(unittest.TestCase):
    """Tests for resolve_path()."""

    def test_dollar_alone_returns_doc(self):
        doc = {"a": 1}
        self.assertEqual(resolve_path(doc, "$"), doc)

    def test_top_level_field(self):
        doc = {"status": "active"}
        self.assertEqual(resolve_path(doc, "$.status"), "active")

    def test_nested_field(self):
        doc = {"address": {"city": "NYC"}}
        self.assertEqual(resolve_path(doc, "$.address.city"), "NYC")

    def test_missing_intermediate_key_returns_none(self):
        doc = {"a": 1}
        self.assertIsNone(resolve_path(doc, "$.missing.field"))

    def test_path_not_starting_with_dollar_returns_none(self):
        doc = {"a": 1}
        self.assertIsNone(resolve_path(doc, "a"))

    def test_path_against_non_dict_returns_none(self):
        self.assertIsNone(resolve_path("hello", "$.field"))

    def test_dollar_alone_with_scalar(self):
        self.assertEqual(resolve_path(42, "$"), 42)


# ===================================================================
# apply_transform
# ===================================================================


class TestApplyTransform(unittest.TestCase):
    """Tests for apply_transform()."""

    # -- String transforms --

    def test_trim(self):
        self.assertEqual(apply_transform("  hello  ", "trim()"), "hello")

    def test_trimStart(self):
        self.assertEqual(apply_transform("  hello  ", "trimStart()"), "hello  ")

    def test_trimEnd(self):
        self.assertEqual(apply_transform("  hello  ", "trimEnd()"), "  hello")

    def test_toUpperCase(self):
        self.assertEqual(apply_transform("hello", "toUpperCase()"), "HELLO")

    def test_toLowerCase(self):
        self.assertEqual(apply_transform("HELLO", "toLowerCase()"), "hello")

    def test_includes(self):
        self.assertTrue(apply_transform("hello world", "includes(,world)"))
        self.assertFalse(apply_transform("hello world", "includes(,xyz)"))

    # -- Numeric transforms --

    def test_parseInt(self):
        self.assertEqual(apply_transform("42", "parseInt()"), 42)

    def test_parseInt_invalid(self):
        self.assertIsNone(apply_transform("abc", "parseInt()"))

    def test_parseFloat(self):
        self.assertAlmostEqual(apply_transform("3.14", "parseFloat()"), 3.14)

    def test_parseFloat_invalid(self):
        self.assertIsNone(apply_transform("abc", "parseFloat()"))

    def test_toFixed(self):
        result = apply_transform("19.99", "toFixed()")
        self.assertEqual(result, Decimal("19.99"))

    def test_toFixed_with_precision(self):
        result = apply_transform("19.999", "toFixed(,2)")
        self.assertEqual(result, Decimal("20.00"))

    def test_toFixed_strips_json_path_arg(self):
        result = apply_transform("19.999", "toFixed($.field,2)")
        self.assertEqual(result, Decimal("20.00"))

    def test_toFixed_invalid(self):
        self.assertIsNone(apply_transform("abc", "toFixed()"))

    def test_toString(self):
        self.assertEqual(apply_transform(42, "toString()"), "42")

    def test_toString_none(self):
        self.assertIsNone(apply_transform(None, "toString()"))

    # -- coalesce --

    def test_coalesce_returns_default_when_none(self):
        self.assertEqual(apply_transform(None, "coalesce(,default)"), "default")

    def test_coalesce_returns_value_when_not_none(self):
        self.assertEqual(apply_transform("exists", "coalesce(,default)"), "exists")

    # -- to_date --

    def test_to_date(self):
        result = apply_transform("2024-06-15", "to_date()")
        self.assertEqual(result, date(2024, 6, 15))

    def test_to_date_invalid(self):
        self.assertIsNone(apply_transform("not-a-date", "to_date()"))

    def test_to_date_none(self):
        self.assertIsNone(apply_transform(None, "to_date()"))

    # -- json_stringify --

    def test_json_stringify(self):
        result = apply_transform({"a": 1}, "json_stringify()")
        self.assertEqual(result, '{"a": 1}')

    def test_json_stringify_none(self):
        self.assertIsNone(apply_transform(None, "json_stringify()"))

    # -- None passthrough --

    def test_none_with_trim(self):
        self.assertIsNone(apply_transform(None, "trim()"))

    def test_none_with_toUpperCase(self):
        self.assertIsNone(apply_transform(None, "toUpperCase()"))

    def test_none_with_parseInt(self):
        self.assertIsNone(apply_transform(None, "parseInt()"))

    # -- Unknown transform --

    def test_unknown_transform_passes_through(self):
        self.assertEqual(apply_transform("val", "unknown_func()"), "val")

    # -- Malformed transform (no parens match) --

    def test_malformed_transform_passes_through(self):
        self.assertEqual(apply_transform("val", "no_parens"), "val")


# ===================================================================
# resolve_column
# ===================================================================


class TestResolveColumn(unittest.TestCase):
    """Tests for resolve_column()."""

    def test_string_col_def(self):
        doc = {"status": "active"}
        self.assertEqual(resolve_column(doc, "$.status"), "active")

    def test_dict_col_def_with_path_and_transform(self):
        doc = {"name": "  Alice  "}
        col_def = {"path": "$.name", "transform": "trim()"}
        self.assertEqual(resolve_column(doc, col_def), "Alice")

    def test_dict_col_def_path_only(self):
        doc = {"score": 100}
        col_def = {"path": "$.score"}
        self.assertEqual(resolve_column(doc, col_def), 100)

    def test_dict_col_def_no_path_defaults_to_dollar(self):
        doc = {"x": 1}
        col_def = {}
        self.assertEqual(resolve_column(doc, col_def), doc)


# ===================================================================
# SqlOperation
# ===================================================================


class TestSqlOperation(unittest.TestCase):
    """Tests for SqlOperation."""

    def test_delete_generates_correct_sql(self):
        op = SqlOperation("DELETE", "orders", where={"doc_id": "order1"})
        sql, params = op.to_sql()
        self.assertIn("DELETE FROM", sql)
        self.assertIn('"orders"', sql)
        self.assertIn('"doc_id" = $1', sql)
        self.assertEqual(params, ["order1"])

    def test_insert_generates_correct_sql(self):
        op = SqlOperation(
            "INSERT", "order_items", data={"order_doc_id": "o1", "qty": 5}
        )
        sql, params = op.to_sql()
        self.assertIn("INSERT INTO", sql)
        self.assertIn('"order_items"', sql)
        self.assertIn("$1", sql)
        self.assertIn("$2", sql)
        self.assertEqual(params, ["o1", 5])

    def test_upsert_generates_on_conflict_sql(self):
        op = SqlOperation(
            "UPSERT",
            "orders",
            data={"doc_id": "o1", "status": "shipped"},
            conflict_column="doc_id",
        )
        sql, params = op.to_sql()
        self.assertIn("INSERT INTO", sql)
        self.assertIn('ON CONFLICT ("doc_id")', sql)
        self.assertIn("DO UPDATE SET", sql)
        self.assertIn('"status" = EXCLUDED."status"', sql)
        self.assertEqual(params, ["o1", "shipped"])

    def test_unknown_op_type_raises(self):
        op = SqlOperation("MERGE", "t")
        with self.assertRaises(ValueError):
            op.to_sql()

    def test_repr_includes_all_fields(self):
        op = SqlOperation(
            "UPSERT",
            "orders",
            data={"doc_id": "o1"},
            where={"doc_id": "o1"},
            conflict_column="doc_id",
        )
        r = repr(op)
        self.assertIn("UPSERT", r)
        self.assertIn("orders", r)
        self.assertIn("data=", r)
        self.assertIn("where=", r)
        self.assertIn("conflict_column=", r)


# ===================================================================
# SchemaMapper
# ===================================================================


def _load_orders_mapping() -> dict:
    with open(ORDERS_MAPPING_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _sample_order_doc() -> dict:
    return {
        "_id": "order::001",
        "_rev": "1-abc",
        "type": "order",
        "status": "pending",
        "customer_id": "cust_42",
        "order_date": "2024-06-15",
        "total": 99.95,
        "items": [
            {"product_id": "WIDGET-A", "qty": 2, "price": 29.99},
            {"product_id": "GADGET-B", "qty": 1, "price": 39.97},
        ],
    }


class TestSchemaMapperMatches(unittest.TestCase):
    """Tests for SchemaMapper.matches()."""

    def test_matches_returns_true_when_doc_matches(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = {"type": "order", "status": "new"}
        self.assertTrue(mapper.matches(doc))

    def test_matches_returns_false_when_doc_does_not_match(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = {"type": "invoice", "status": "new"}
        self.assertFalse(mapper.matches(doc))

    def test_matches_returns_true_when_no_filter(self):
        mapping = {"source": {}, "tables": []}
        mapper = SchemaMapper(mapping)
        self.assertTrue(mapper.matches({"anything": True}))

    # -- expression-based matching -------------------------------------------

    def test_matches_expression_split_index(self):
        """split($._id,"::")[0] extracts the prefix from the doc key."""
        m = SchemaMapper(
            {
                "source": {
                    "match": {"expression": 'split($._id,"::")[0]', "value": "invoice"}
                }
            }
        )
        self.assertTrue(m.matches({"_id": "invoice::12345"}))
        self.assertFalse(m.matches({"_id": "order::12345"}))
        self.assertFalse(m.matches({"_id": "invoice_no_separator"}))

    def test_matches_expression_split_second_part(self):
        """split($._id,"::")[1] extracts the second segment."""
        m = SchemaMapper(
            {"source": {"match": {"expression": 'split($._id,"::")[1]', "value": "99"}}}
        )
        self.assertTrue(m.matches({"_id": "type::99"}))
        self.assertFalse(m.matches({"_id": "type::100"}))

    def test_matches_expression_lowercase(self):
        m = SchemaMapper(
            {
                "source": {
                    "match": {
                        "expression": "toLowerCase($._id)",
                        "value": "invoice::abc",
                    }
                }
            }
        )
        self.assertTrue(m.matches({"_id": "INVOICE::ABC"}))
        self.assertFalse(m.matches({"_id": "ORDER::ABC"}))

    def test_matches_expression_plain_path(self):
        """Expression can also be a plain JSON path."""
        m = SchemaMapper(
            {"source": {"match": {"expression": "$._id", "value": "invoice::1"}}}
        )
        self.assertTrue(m.matches({"_id": "invoice::1"}))
        self.assertFalse(m.matches({"_id": "invoice::2"}))

    def test_matches_expression_missing_field(self):
        m = SchemaMapper(
            {
                "source": {
                    "match": {"expression": 'split($._id,"::")[0]', "value": "invoice"}
                }
            }
        )
        self.assertFalse(m.matches({"no_id": "x"}))


class TestSchemaMapperMapDocument(unittest.TestCase):
    """Tests for SchemaMapper.map_document()."""

    def test_upsert_parent_only_table(self):
        mapping = {
            "source": {},
            "tables": [
                {
                    "name": "customers",
                    "primary_key": "id",
                    "columns": {"id": "$._id", "name": "$.name"},
                }
            ],
        }
        mapper = SchemaMapper(mapping)
        doc = {"_id": "c1", "name": "Alice"}
        ops, _diag = mapper.map_document(doc)
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0].op_type, "UPSERT")
        self.assertEqual(ops[0].table, "customers")
        self.assertEqual(ops[0].data, {"id": "c1", "name": "Alice"})

    def test_delete_parent_only_table(self):
        mapping = {
            "source": {},
            "tables": [
                {
                    "name": "customers",
                    "primary_key": "id",
                    "columns": {"id": "$._id", "name": "$.name"},
                }
            ],
        }
        mapper = SchemaMapper(mapping)
        doc = {"_id": "c1", "name": "Alice"}
        ops, _diag = mapper.map_document(doc, is_delete=True)
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0].op_type, "DELETE")
        self.assertEqual(ops[0].where, {"id": "c1"})

    def test_child_table_source_array_expands(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = _sample_order_doc()
        ops, _diag = mapper.map_document(doc)

        # 1 UPSERT for orders parent
        # 1 DELETE for order_items (delete_insert strategy)
        # 2 INSERTs for order_items (one per array item)
        upserts = [o for o in ops if o.op_type == "UPSERT"]
        deletes = [o for o in ops if o.op_type == "DELETE"]
        inserts = [o for o in ops if o.op_type == "INSERT"]
        self.assertEqual(len(upserts), 1)
        self.assertEqual(upserts[0].table, "orders")
        self.assertEqual(len(deletes), 1)
        self.assertEqual(deletes[0].table, "order_items")
        self.assertEqual(len(inserts), 2)
        for ins in inserts:
            self.assertEqual(ins.table, "order_items")

    def test_delete_insert_strategy_generates_delete_before_inserts(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = _sample_order_doc()
        ops, _diag = mapper.map_document(doc)

        # Find indices of the order_items ops
        item_ops = [(i, o) for i, o in enumerate(ops) if o.table == "order_items"]
        # First order_items op should be DELETE
        self.assertEqual(item_ops[0][1].op_type, "DELETE")
        # Subsequent should be INSERT
        for _, o in item_ops[1:]:
            self.assertEqual(o.op_type, "INSERT")

    def test_delete_skips_tables_with_on_delete_ignore(self):
        mapping = {
            "source": {},
            "tables": [
                {
                    "name": "parent_tbl",
                    "primary_key": "id",
                    "columns": {"id": "$._id"},
                    "on_delete": "ignore",
                },
            ],
        }
        mapper = SchemaMapper(mapping)
        ops, _diag = mapper.map_document({"_id": "x"}, is_delete=True)
        self.assertEqual(len(ops), 0)

    def test_delete_child_on_delete_ignore(self):
        mapping = {
            "source": {},
            "tables": [
                {
                    "name": "parent_tbl",
                    "primary_key": "id",
                    "columns": {"id": "$._id"},
                },
                {
                    "name": "child_tbl",
                    "primary_key": "id",
                    "columns": {"id": "$._id"},
                    "parent": "parent_tbl",
                    "foreign_key": {"column": "parent_id", "references": "id"},
                    "on_delete": "ignore",
                },
            ],
        }
        mapper = SchemaMapper(mapping)
        ops, _diag = mapper.map_document({"_id": "x"}, is_delete=True)
        # Only the parent DELETE should be present
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0].table, "parent_tbl")

    def test_array_child_rows_resolve_item_then_parent(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = _sample_order_doc()
        ops, _diag = mapper.map_document(doc)

        inserts = [o for o in ops if o.op_type == "INSERT"]
        # order_doc_id should come from the parent doc's _id
        for ins in inserts:
            self.assertEqual(ins.data["order_doc_id"], "order::001")
        # product_id is sourced from the item with toLowerCase() applied per
        # the orders mapping
        self.assertEqual(inserts[0].data["product_id"], "widget-a")
        self.assertEqual(inserts[1].data["product_id"], "gadget-b")

    def test_upsert_with_orders_mapping_applies_transforms(self):
        mapper = SchemaMapper(_load_orders_mapping())
        doc = _sample_order_doc()
        ops, _diag = mapper.map_document(doc)

        parent_op = [o for o in ops if o.table == "orders"][0]
        # order_date should be transformed via to_date()
        self.assertEqual(parent_op.data["order_date"], date(2024, 6, 15))


class TestSchemaMapperFromFile(unittest.TestCase):
    """Tests for SchemaMapper.from_file()."""

    def test_from_file_loads_mapping(self):
        mapper = SchemaMapper.from_file(ORDERS_MAPPING_PATH)
        self.assertTrue(mapper.matches({"type": "order"}))
        self.assertFalse(mapper.matches({"type": "invoice"}))
        self.assertEqual(len(mapper.tables), 2)

    def test_from_file_with_temp_file(self):
        mapping = {
            "source": {"match": {"field": "kind", "value": "test"}},
            "tables": [
                {
                    "name": "test_tbl",
                    "primary_key": "id",
                    "columns": {"id": "$._id"},
                }
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(mapping, f)
            f.flush()
            tmp_path = f.name

        try:
            mapper = SchemaMapper.from_file(tmp_path)
            self.assertTrue(mapper.matches({"kind": "test"}))
            self.assertFalse(mapper.matches({"kind": "other"}))
        finally:
            os.unlink(tmp_path)


if __name__ == "__main__":
    unittest.main()
