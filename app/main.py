from datetime import datetime
import time

from agents.source_agent import SourceAgent
from agents.schema_agent import SchemaAgent
from agents.semantic_agent import SemanticAgent
from agents.data_reader_agent import DataReaderAgent
from agents.data_quality_agent import DataQualityAgent
from agents.validation_agent import ValidationAgent
from agents.transformation_agent import TransformationAgent
from agents.gold_agent import GoldAgent
from agents.pipeline_report import PipelineReport
from agents.error_handler_agent import ErrorHandlerAgent
from agents.decision_agent import DecisionAgent


# ============================================================
# DE-JARVIS CONFIGURATION
# ============================================================

PROJECT_NAME = "DE-JARVIS"
VERSION = "2.9"


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():

    start_time = datetime.now()
    pipeline_start = time.time()

    print()
    print("=" * 70)
    print("                    DE-JARVIS")
    print("=" * 70)
    print("        Data Engineering Automation System")
    print(f"        Version : {VERSION}")
    print(
        f"        Started : "
        f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print("        Status  : ONLINE")
    print("=" * 70)

    # ========================================================
    # INITIALIZE AGENTS
    # ========================================================

    source_agent = SourceAgent()
    schema_agent = SchemaAgent()
    reader = DataReaderAgent()
    semantic_agent = SemanticAgent()
    quality_agent = DataQualityAgent()
    validator = ValidationAgent()
    transformer = TransformationAgent()
    gold_agent = GoldAgent()
    error_handler = ErrorHandlerAgent()
    decision_agent = DecisionAgent()
    report = PipelineReport()

    # ========================================================
    # COUNTERS
    # ========================================================

    total_files = 0
    successful_files = 0
    failed_files = 0
    total_rows = 0

    # ========================================================
    # HEADER
    # ========================================================

    def print_agent_header(
        number,
        name
    ):

        print()
        print("-" * 70)
        print(
            f"[{number}] {name}"
        )
        print("-" * 70)

    # ========================================================
    # ERROR
    # ========================================================

    def add_error(
        agent_name,
        file_name,
        error
    ):

        try:

            error_handler.log_error(
                agent_name,
                f"{file_name}: {error}"
            )

        except Exception:

            pass

        report.add_step(
            f"{agent_name} - {file_name}",
            False,
            str(error)
        )

    # ========================================================
    # 1. SOURCE
    # ========================================================

    print_agent_header(
        1,
        "SOURCE AGENT"
    )

    try:

        files = source_agent.scan()

        if not files:

            print(
                "DE-JARVIS: "
                "No CSV files found."
            )

            report.add_step(
                "Source Agent",
                False,
                "No CSV files found"
            )

            return False

        total_files = len(files)

        print(
            f"SOURCE AGENT: "
            f"{total_files} CSV file(s) detected"
        )

        report.add_step(
            "Source Agent",
            True,
            f"{total_files} CSV file(s) detected"
        )

    except Exception as e:

        print(
            f"SOURCE AGENT ERROR: {e}"
        )

        add_error(
            "Source Agent",
            "SYSTEM",
            e
        )

        return False

    # ========================================================
    # FILE LOOP
    # ========================================================

    for file_number, input_file in enumerate(
        files,
        start=1
    ):

        file_start = time.time()

        print()
        print("=" * 70)
        print(
            f"PROCESSING FILE "
            f"{file_number}/{total_files}: "
            f"{input_file.name}"
        )
        print("=" * 70)

        # ====================================================
        # 2. SCHEMA
        # ====================================================

        print_agent_header(
            2,
            "SCHEMA AGENT"
        )

        try:

            schema = schema_agent.analyze(
                str(input_file)
            )

            print(
                f"SCHEMA AGENT: "
                f"{schema['column_count']} "
                f"columns detected"
            )

            print(
                f"SCHEMA AGENT: "
                f"{schema['row_count']} "
                f"rows detected"
            )

            print()
            print("DETECTED SCHEMA")
            print("-" * 70)

            for column in schema["columns"]:

                print(
                    f"Column: {column['name']} "
                    f"| Type: {column['type']} "
                    f"| Nullable: "
                    f"{column['nullable']}"
                )

            report.add_step(
                f"Schema Agent - "
                f"{input_file.name}",
                True,
                (
                    f"{schema['column_count']} "
                    f"columns, "
                    f"{schema['row_count']} "
                    f"rows detected"
                )
            )

        except Exception as e:

            print(
                f"SCHEMA AGENT ERROR: {e}"
            )

            add_error(
                "Schema Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 3. DATA READER
        # ====================================================

        print_agent_header(
            3,
            "DATA READER AGENT"
        )

        try:

            data = reader.read_csv(
                str(input_file)
            )

            if not data:

                add_error(
                    "Data Reader Agent",
                    input_file.name,
                    "No data loaded"
                )

                failed_files += 1
                continue

            row_count = len(data)

            total_rows += row_count

            print(
                f"DATA READER AGENT: "
                f"{row_count} rows loaded"
            )

            print()
            print("DATA SAMPLE")
            print("-" * 70)

            for row in data[:3]:

                print(row)

            report.add_step(
                f"Data Reader - "
                f"{input_file.name}",
                True,
                f"{row_count} rows loaded"
            )

        except Exception as e:

            print(
                f"DATA READER ERROR: {e}"
            )

            add_error(
                "Data Reader Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 4. SEMANTIC INTELLIGENCE v2.8
        # ====================================================

        print_agent_header(
            4,
            "SEMANTIC INTELLIGENCE AGENT"
        )

        try:

            semantic_schema = semantic_agent.analyze(
                schema,
                data=data
            )

            print()
            print("SEMANTIC SCHEMA")
            print("-" * 70)

            for column in semantic_schema["columns"]:

                print(
                    f"Column      : "
                    f"{column['name']}"
                )

                print(
                    f"Type        : "
                    f"{column['type']}"
                )

                print(
                    f"Role        : "
                    f"{column.get('role', 'UNKNOWN')}"
                )

                print(
                    f"Confidence  : "
                    f"{column.get('confidence', 0):.2f}"
                )

                print(
                    f"Cardinality : "
                    f"{column.get('cardinality', 0)}"
                )

                print(
                    f"Unique Ratio: "
                    f"{column.get('unique_ratio', 0):.2f}"
                )

                print(
                    f"Null Rate   : "
                    f"{column.get('null_rate', 0):.2f}"
                )

                print(
                    f"Reason      : "
                    f"{column.get('reason', '')}"
                )

                print("-" * 70)

            schema = semantic_schema

            report.add_step(
                f"Semantic Agent - "
                f"{input_file.name}",
                True,
                (
                    f"{schema['column_count']} "
                    f"columns classified"
                )
            )

        except Exception as e:

            print(
                f"SEMANTIC AGENT ERROR: {e}"
            )

            add_error(
                "Semantic Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 5. DATA QUALITY
        # ====================================================

        print_agent_header(
            5,
            "DATA QUALITY AGENT"
        )

        try:

            quality_result = quality_agent.analyze(
                data,
                schema
            )

            print(
                f"DATA QUALITY AGENT: "
                f"Score = "
                f"{quality_result['score']:.2f}%"
            )

            print(
                f"DATA QUALITY AGENT: "
                f"Status = "
                f"{quality_result['status']}"
            )

            if quality_result["status"] == "FAIL":

                print(
                    "DATA QUALITY AGENT: "
                    "QUALITY CHECK FAILED"
                )

                report.add_step(
                    f"Data Quality - "
                    f"{input_file.name}",
                    False,
                    "Quality check failed"
                )

                failed_files += 1
                continue

            report.add_step(
                f"Data Quality - "
                f"{input_file.name}",
                True,
                (
                    f"Quality score "
                    f"{quality_result['score']:.2f}%"
                )
            )

        except Exception as e:

            print(
                f"DATA QUALITY ERROR: {e}"
            )

            add_error(
                "Data Quality Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 6. VALIDATION
        # ====================================================

        print_agent_header(
            6,
            "DYNAMIC VALIDATION AGENT"
        )

        try:

            validation_result = validator.validate(
                data,
                schema
            )

            if not validation_result:

                print(
                    "VALIDATION AGENT: "
                    "VALIDATION FAILED"
                )

                report.add_step(
                    f"Validation - "
                    f"{input_file.name}",
                    False,
                    "Validation failed"
                )

                failed_files += 1
                continue

            print(
                "VALIDATION AGENT: "
                "VALIDATION PASSED"
            )

            report.add_step(
                f"Validation - "
                f"{input_file.name}",
                True,
                "Dynamic validation passed"
            )

        except Exception as e:

            print(
                f"VALIDATION ERROR: {e}"
            )

            add_error(
                "Validation Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 7. AI TRANSFORMATION INTELLIGENCE v2.9
        # ====================================================

        print_agent_header(
            7,
            "AI TRANSFORMATION INTELLIGENCE v2.9"
        )

        try:

            transformed_data = transformer.transform(
                data,
                schema
            )

            silver_file = (
                f"data/silver/"
                f"{input_file.stem}.csv"
            )

            transformer.save(
                transformed_data,
                silver_file
            )

            print()
            print(
                "TRANSFORMATION AGENT: "
                "Transformation completed"
            )

            print(
                f"TRANSFORMATION AGENT: "
                f"{len(transformed_data)} "
                f"rows transformed"
            )

            print(
                f"TRANSFORMATION AGENT: "
                f"Saved to {silver_file}"
            )

            report.add_step(
                f"Transformation - "
                f"{input_file.name}",
                True,
                f"Saved to {silver_file}"
            )

        except Exception as e:

            print(
                f"TRANSFORMATION ERROR: {e}"
            )

            add_error(
                "Transformation Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # 8. GOLD
        # ====================================================

        print_agent_header(
            8,
            "SEMANTIC GOLD / ANALYTICS AGENT"
        )

        try:

            gold_file = (
                f"data/gold/"
                f"{input_file.stem}_summary.csv"
            )

            result = gold_agent.aggregate(
                silver_file,
                gold_file,
                schema
            )

            print(
                "GOLD AGENT: "
                "Semantic analytics completed"
            )

            print(
                f"GOLD AGENT: "
                f"{len(result)} "
                f"analytics rows generated"
            )

            print(
                f"GOLD AGENT: "
                f"Saved to {gold_file}"
            )

            report.add_step(
                f"Gold Agent - "
                f"{input_file.name}",
                True,
                f"Saved to {gold_file}"
            )

        except Exception as e:

            print(
                f"GOLD AGENT ERROR: {e}"
            )

            add_error(
                "Gold Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # GOLD PREVIEW
        # ====================================================

        print()
        print("GOLD ANALYTICS DATA")
        print("-" * 70)

        for row in result:

            print(row)

        # ====================================================
        # 9. DECISION
        # ====================================================

        print_agent_header(
            9,
            "DECISION AGENT"
        )

        try:

            analysis = decision_agent.analyze(
                data,
                result
            )

            decision_agent.show_report(
                analysis
            )

            report.add_step(
                f"Decision Agent - "
                f"{input_file.name}",
                True,
                "Pipeline output analyzed"
            )

        except Exception as e:

            print(
                f"DECISION AGENT ERROR: {e}"
            )

            add_error(
                "Decision Agent",
                input_file.name,
                e
            )

            failed_files += 1
            continue

        # ====================================================
        # FILE COMPLETE
        # ====================================================

        file_time = (
            time.time()
            - file_start
        )

        successful_files += 1

        print()
        print("=" * 70)
        print(
            f"FILE COMPLETE: "
            f"{input_file.name}"
        )

        print(
            f"Processing Time: "
            f"{file_time:.2f}s"
        )

        print("=" * 70)

    # ========================================================
    # PIPELINE SUMMARY
    # ========================================================

    pipeline_time = (
        time.time()
        - pipeline_start
    )

    print()
    print()
    print("=" * 70)
    print("                    PIPELINE COMPLETE")
    print("=" * 70)

    print()
    print("PIPELINE SUMMARY")
    print("-" * 70)

    print(
        f"Total CSV Files      : "
        f"{total_files}"
    )

    print(
        f"Successful Files     : "
        f"{successful_files}"
    )

    print(
        f"Failed Files         : "
        f"{failed_files}"
    )

    print(
        f"Total Rows Processed : "
        f"{total_rows}"
    )

    print(
        f"Pipeline Runtime     : "
        f"{pipeline_time:.2f}s"
    )

    print()
    print("-" * 70)

    if failed_files == 0:

        print(
            "FINAL STATUS: SUCCESS"
        )

    else:

        print(
            "FINAL STATUS: "
            "COMPLETED WITH ERRORS"
        )

    print("-" * 70)

    # ========================================================
    # FINAL REPORT
    # ========================================================

    print()
    print("=" * 70)
    print("                 FINAL PIPELINE REPORT")
    print("=" * 70)

    report.show()

    # ========================================================
    # SHUTDOWN
    # ========================================================

    end_time = datetime.now()

    print()
    print("=" * 70)
    print("                 DE-JARVIS SHUTDOWN")
    print("=" * 70)

    print(
        f"Started : "
        f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        f"Ended   : "
        f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        f"Runtime : "
        f"{pipeline_time:.2f} seconds"
    )

    print("Status  : OFFLINE")

    print("=" * 70)

    return failed_files == 0


# ============================================================
# DIRECT EXECUTION
# ============================================================

if __name__ == "__main__":

    run_pipeline()