from agents.schema_agent import SchemaAgent
from agents.semantic_agent import SemanticAgent
from agents.data_quality_agent import DataQualityAgent
from agents.validation_agent import ValidationAgent
from agents.transformation_agent import TransformationAgent
from agents.gold_agent import GoldAgent

from tools.file_tools import read_csv, resolve_csv_file


# =========================================================
# AGENTS
# =========================================================

_schema_agent = SchemaAgent()
_semantic_agent = SemanticAgent()
_data_quality_agent = DataQualityAgent()
_validation_agent = ValidationAgent()
_transformation_agent = TransformationAgent()
_gold_agent = GoldAgent()


# =========================================================
# DUPLICATE TOOLS
# =========================================================

def remove_duplicates(data):

    if not data:
        return {
            "status": "SUCCESS",
            "removed_count": 0,
            "original_count": 0,
            "final_count": 0,
            "data": []
        }

    seen = set()
    cleaned_data = []
    removed_count = 0

    for row in data:

        row_key = tuple(sorted(row.items()))

        if row_key in seen:
            removed_count += 1
            continue

        seen.add(row_key)
        cleaned_data.append(row)

    return {
        "status": "SUCCESS",
        "removed_count": removed_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
        "data": cleaned_data
    }


def check_duplicates(data):

    if not data:
        return {
            "status": "SUCCESS",
            "duplicate_count": 0,
            "has_duplicates": False
        }

    seen = set()
    duplicate_count = 0

    for row in data:

        row_key = tuple(sorted(row.items()))

        if row_key in seen:
            duplicate_count += 1
        else:
            seen.add(row_key)

    return {
        "status": "SUCCESS",
        "duplicate_count": duplicate_count,
        "has_duplicates": duplicate_count > 0
    }


# =========================================================
# FILE TOOLS
# =========================================================

def resolve_csv(file_name):

    file_path = resolve_csv_file(file_name)

    return {
        "status": "SUCCESS",
        "file": file_name,
        "path": file_path
    }


def load_csv(file_name):

    file_path = resolve_csv_file(file_name)

    data = read_csv(file_path)

    return {
        "status": "SUCCESS",
        "file": file_name,
        "path": file_path,
        "row_count": len(data),
        "data": data
    }


# =========================================================
# SCHEMA
# =========================================================

def schema_analysis(file_path):

    return _schema_agent.analyze(file_path)


# =========================================================
# SEMANTIC ANALYSIS
# =========================================================

def semantic_analysis(schema, data=None):

    return _semantic_agent.analyze(
        schema,
        data=data
    )


# =========================================================
# DATA QUALITY
# =========================================================

def data_quality(data, schema):

    return _data_quality_agent.analyze(
        data,
        schema
    )


# =========================================================
# VALIDATION
# =========================================================

def validate_data(data, schema=None):

    validation_result = _validation_agent.validate(
        data,
        schema
    )

    return {
        "status": "SUCCESS" if validation_result else "FAIL",
        "valid": validation_result
    }


# =========================================================
# TRANSFORMATION
# =========================================================

def transform_data(data, schema):

    transformed_data = _transformation_agent.transform(
        data,
        schema
    )

    return {
        "status": "SUCCESS",
        "data": transformed_data,
        "row_count": len(transformed_data),
        "rules": _transformation_agent.get_rules()
    }


# =========================================================
# SAVE SILVER DATA
# =========================================================

def save_transformed_data(data, output_path):

    _transformation_agent.save(
        data,
        output_path
    )

    return {
        "status": "SUCCESS",
        "path": output_path,
        "row_count": len(data)
    }


# =========================================================
# GOLD ANALYTICS
# =========================================================

def gold_analytics(silver_file, gold_file, schema=None):

    analytics = _gold_agent.aggregate(
        silver_file,
        gold_file,
        schema
    )

    return {
        "status": "SUCCESS",
        "path": gold_file,
        "row_count": len(analytics),
        "data": analytics
    }


# =========================================================
# TOOL REGISTRATION
# =========================================================

def register_data_tools(registry):

    # -----------------------------------------------------
    # FILE TOOLS
    # -----------------------------------------------------

    registry.register(
        "RESOLVE_CSV",
        "Find and resolve a CSV file in the raw data directory.",
        resolve_csv
    )

    registry.register(
        "READ_CSV",
        "Read a CSV file into JARVIS memory.",
        load_csv
    )

    # -----------------------------------------------------
    # DUPLICATE TOOLS
    # -----------------------------------------------------

    registry.register(
        "REMOVE_DUPLICATES",
        "Remove duplicate rows from a dataset.",
        remove_duplicates
    )

    registry.register(
        "CHECK_DUPLICATES",
        "Check a dataset for duplicate rows.",
        check_duplicates
    )

    # -----------------------------------------------------
    # SCHEMA
    # -----------------------------------------------------

    registry.register(
        "SCHEMA_ANALYSIS",
        "Analyze CSV schema, detect columns, row count, data types and nullability.",
        schema_analysis
    )

    # -----------------------------------------------------
    # SEMANTIC
    # -----------------------------------------------------

    registry.register(
        "SEMANTIC_ANALYSIS",
        "Analyze business meaning of columns using names, types and sample values.",
        semantic_analysis
    )

    # -----------------------------------------------------
    # DATA QUALITY
    # -----------------------------------------------------

    registry.register(
        "DATA_QUALITY",
        "Run dynamic data quality checks including nulls, duplicates, IDs, types and column consistency.",
        data_quality
    )

    # -----------------------------------------------------
    # VALIDATION
    # -----------------------------------------------------

    registry.register(
        "VALIDATE_DATA",
        "Validate dataset against schema, data types, nulls, column consistency, semantic keys and duplicate keys.",
        validate_data
    )

    # -----------------------------------------------------
    # TRANSFORMATION
    # -----------------------------------------------------

    registry.register(
        "TRANSFORMATION",
        "Transform data using semantic roles and transformation rules.",
        transform_data
    )

    # -----------------------------------------------------
    # SAVE SILVER
    # -----------------------------------------------------

    registry.register(
        "SAVE_SILVER",
        "Save transformed data into the Silver data layer.",
        save_transformed_data
    )

    # -----------------------------------------------------
    # GOLD
    # -----------------------------------------------------

    registry.register(
        "GOLD_ANALYTICS",
        "Generate aggregated analytics from the Silver data layer and save them into the Gold data layer.",
        gold_analytics
    )