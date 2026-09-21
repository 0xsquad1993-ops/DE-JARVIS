from pathlib import Path
from datetime import datetime
import csv
from collections import defaultdict



class GoldAgent:
    """
    DE-JARVIS GOLD ENGINE v4.0

    Outputs:
      1. <source>_detail.csv   -> transaction/detail Gold
      2. <source>.csv          -> analytics/business Gold

    Analytics capabilities:
      - row count
      - SUM / AVG / MIN / MAX
      - COUNT DISTINCT
      - KPI metrics
      - percentage share
      - ranking
      - top/bottom flags
      - median / stddev / percentiles
      - anomaly flags
      - reconciliation
      - Gold data-quality checks
      - automatic business insight text
    """

    VERSION = "5.0"

    def aggregate(self, silver_file, gold_file, schema=None):
        silver_file = Path(silver_file)
        gold_file = Path(gold_file)
        gold_file.parent.mkdir(parents=True, exist_ok=True)

        print("GOLD AGENT v5.0: Reading silver data...")

        data = self._read_csv(silver_file)

        if not data:
            self.save([], gold_file)
            detail_file = gold_file.parent / f"{silver_file.stem}_detail.csv"
            self.save([], detail_file)
            return []

        # --------------------------------------------------
        # SEMANTIC ROLES
        # --------------------------------------------------
        inferred = schema or self._infer_schema(data)

        key_columns = []
        dimension_columns = []
        measure_columns = []
        attribute_columns = []

        for column in inferred.get("columns", []):
            name = column.get("name")
            role = str(column.get("role", "")).upper()
            key_type = str(column.get("key_type", "")).upper()

            if role == "KEY":
                key_columns.append(name)
            elif role == "DIMENSION":
                dimension_columns.append(name)
            elif role == "MEASURE":
                measure_columns.append(name)
            elif role == "ATTRIBUTE":
                attribute_columns.append(name)
            else:
                detected = str(column.get("type", "")).lower()
                if detected in {"integer", "float"}:
                    measure_columns.append(name)
                else:
                    dimension_columns.append(name)

        print(f"GOLD AGENT v5.0: Keys = {key_columns}")
        print(f"GOLD AGENT v5.0: Dimensions = {dimension_columns}")
        print(f"GOLD AGENT v5.0: Attributes = {attribute_columns}")
        print(f"GOLD AGENT v5.0: Measures = {measure_columns}")

        # --------------------------------------------------
        # DETAIL GOLD
        # --------------------------------------------------
        detail_file = gold_file.parent / f"{silver_file.stem}_detail.csv"
        self.save(data, detail_file)

        print(
            f"GOLD AGENT v5.0: Detail Gold saved to "
            f"{detail_file}"
        )
        print(
            f"GOLD AGENT v5.0: {len(data)} detail rows preserved"
        )

        # --------------------------------------------------
        # ANALYTICS GOLD
        # --------------------------------------------------
        analytics = self._build_analytics(
            data=data,
            dimensions=dimension_columns,
            measures=measure_columns,
            attributes=attribute_columns,
        )

        # --------------------------------------------------
        # DATE/TIME INTELLIGENCE
        # --------------------------------------------------
        detected_date_column = self.add_date_time_intelligence(
            analytics,
            data,
        )

        if detected_date_column:
            print(
                f"GOLD AGENT v5.0: Date column detected = "
                f"{detected_date_column}"
            )

            time_series = self.build_time_series(
                data,
                detected_date_column,
                measure_columns,
            )

            self.add_growth_metrics(
                time_series,
                measure_columns,
            )

            self.year_over_year(
                time_series,
                measure_columns,
            )

            trend_summary = self.detect_trends(
                time_series,
                measure_columns,
            )

            print(
                f"GOLD AGENT v5.0: Time-series periods = "
                f"{len(time_series)}"
            )

            print(
                f"GOLD AGENT v5.0: Trend summary = "
                f"{trend_summary}"
            )

            # Persist time-series output alongside the standard analytics file.
            time_series_file = (
                gold_file.parent
                / f"{silver_file.stem}_time_series.csv"
            )

            self.save(
                time_series,
                time_series_file,
            )

            print(
                f"GOLD AGENT v5.0: Time Series Gold saved to "
                f"{time_series_file}"
            )

        # --------------------------------------------------
        # GOLD QUALITY
        # --------------------------------------------------
        quality = self.gold_quality(analytics)

        # --------------------------------------------------
        # RECONCILIATION
        # --------------------------------------------------
        reconciliation = self.reconcile(
            source_rows=len(data),
            detail_rows=len(data),
            analytics_rows=len(analytics),
        )

        # Add engine metadata to each analytics row.
        for row in analytics:
            row["_gold_engine_version"] = self.VERSION
            row["_gold_quality_status"] = quality["status"]
            row["_reconciliation_status"] = reconciliation["status"]

        self.save(analytics, gold_file)

        print(
            f"GOLD AGENT v5.0: "
            f"{len(analytics)} analytics rows generated"
        )
        print(
            f"GOLD AGENT v5.0: Analytics Gold saved to "
            f"{gold_file}"
        )
        print(
            f"GOLD AGENT v5.0: Quality = "
            f"{quality['status']} ({quality['score']:.2f}%)"
        )
        print(
            f"GOLD AGENT v5.0: Reconciliation = "
            f"{reconciliation['status']}"
        )

        insights = self.generate_insights(
            analytics,
            dimensions=dimension_columns,
            measures=measure_columns,
        )

        if insights:
            print("GOLD BUSINESS INSIGHTS")
            for insight in insights:
                print(f"  - {insight}")

        return analytics

    # ======================================================
    # ANALYTICS ENGINE
    # ======================================================

    def _build_analytics(
        self,
        data,
        dimensions,
        measures,
        attributes,
    ):
        if dimensions:
            groups = {}

            for row in data:
                key = tuple(row.get(column) for column in dimensions)
                groups.setdefault(key, []).append(row)

            analytics = []

            for key, rows in groups.items():
                result = {}

                for index, column in enumerate(dimensions):
                    result[column] = key[index]

                result["row_count"] = len(rows)

                # -------------------------------
                # MEASURE STATISTICS
                # -------------------------------
                for measure in measures:
                    values = [
                        self._to_number(row.get(measure))
                        for row in rows
                    ]
                    values = [
                        value for value in values
                        if value is not None
                    ]

                    if not values:
                        continue

                    result[f"{measure}_sum"] = sum(values)
                    result[f"{measure}_avg"] = (
                        sum(values) / len(values)
                    )
                    result[f"{measure}_min"] = min(values)
                    result[f"{measure}_max"] = max(values)
                    result[f"{measure}_median"] = self._median(values)
                    result[f"{measure}_stddev"] = self._stddev(values)

                    result[f"{measure}_p25"] = self._percentile(
                        values, 25
                    )
                    result[f"{measure}_p75"] = self._percentile(
                        values, 75
                    )
                    result[f"{measure}_p95"] = self._percentile(
                        values, 95
                    )

                # -------------------------------
                # ATTRIBUTE CARDINALITY
                # -------------------------------
                for attribute in attributes:
                    distinct = self.count_distinct(
                        rows,
                        attribute,
                    )
                    result[f"{attribute}_distinct_count"] = distinct

                analytics.append(result)

        else:
            result = {"row_count": len(data)}

            for measure in measures:
                values = [
                    self._to_number(row.get(measure))
                    for row in data
                ]
                values = [
                    value for value in values
                    if value is not None
                ]

                if not values:
                    continue

                result[f"{measure}_sum"] = sum(values)
                result[f"{measure}_avg"] = sum(values) / len(values)
                result[f"{measure}_min"] = min(values)
                result[f"{measure}_max"] = max(values)
                result[f"{measure}_median"] = self._median(values)
                result[f"{measure}_stddev"] = self._stddev(values)
                result[f"{measure}_p25"] = self._percentile(values, 25)
                result[f"{measure}_p75"] = self._percentile(values, 75)
                result[f"{measure}_p95"] = self._percentile(values, 95)

            analytics = [result]

        # ==================================================
        # DISTINCT COUNTS
        # ==================================================
        for attribute in attributes:
            distinct = self.count_distinct(data, attribute)

            if len(analytics) == 1 and not dimensions:
                analytics[0][f"{attribute}_distinct_total"] = distinct

        # ==================================================
        # PERCENTAGE SHARE
        # ==================================================
        self.add_percentage_share(
            analytics,
            measures,
        )

        # ==================================================
        # RANKING / TOP / BOTTOM
        # ==================================================
        self.add_rankings(
            analytics,
            measures,
        )

        # ==================================================
        # ANOMALY FLAGS
        # ==================================================
        self.add_anomaly_flags(
            analytics,
            measures,
        )

        # ==================================================
        # BUSINESS KPI
        # ==================================================
        self.add_kpis(
            analytics,
            measures,
        )

        return analytics

    # ======================================================
    # DATE / TIME INTELLIGENCE v5.0
    # ======================================================

    def add_date_time_intelligence(self, rows, data):
        """
        Detect a usable date/time column and add:
          - year
          - quarter
          - month
          - month_name
          - week
          - day
          - day_of_week
          - year_month
          - year_quarter

        This is additive and only runs when a usable date column exists.
        """
        date_column = self._detect_date_column(data)

        if not date_column:
            return None

        parsed = []

        for row in data:
            dt = self._parse_date(row.get(date_column))
            if dt:
                parsed.append(dt)

        if not parsed:
            return None

        # For analytics rows, add calendar metadata only when the same
        # date column exists in the grouped output.
        for row in rows:
            raw = row.get(date_column)

            if raw in (None, ""):
                continue

            dt = self._parse_date(raw)

            if not dt:
                continue

            row["date_year"] = dt.year
            row["date_quarter"] = f"Q{((dt.month - 1) // 3) + 1}"
            row["date_month"] = dt.month
            row["date_month_name"] = dt.strftime("%B")
            row["date_week"] = int(dt.strftime("%V"))
            row["date_day"] = dt.day
            row["date_day_of_week"] = dt.strftime("%A")
            row["date_year_month"] = dt.strftime("%Y-%m")
            row["date_year_quarter"] = (
                f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"
            )

        return date_column

    def build_time_series(self, data, date_column, measures):
        """Build monthly time-series analytics from detail data."""
        buckets = {}

        for row in data:
            dt = self._parse_date(row.get(date_column))

            if not dt:
                continue

            period = dt.strftime("%Y-%m")
            buckets.setdefault(period, [])

            buckets[period].append(row)

        result = []

        for period in sorted(buckets):
            rows = buckets[period]

            item = {
                "period": period,
                "year": int(period[:4]),
                "month": int(period[5:7]),
                "row_count": len(rows),
            }

            for measure in measures:
                values = [
                    self._to_number(row.get(measure))
                    for row in rows
                ]
                values = [x for x in values if x is not None]

                if not values:
                    continue

                item[f"{measure}_sum"] = sum(values)
                item[f"{measure}_avg"] = (
                    sum(values) / len(values)
                )

            result.append(item)

        return result

    def add_growth_metrics(self, rows, measures):
        """
        Add period-over-period growth to an ordered time series.
        The same engine supports WoW/MoM-style period data; the
        generated period is monthly when build_time_series() is used.
        """
        if not rows:
            return

        for measure in measures:
            key = f"{measure}_sum"

            if key not in rows[0]:
                continue

            previous = None

            for row in rows:
                current = self._to_number(row.get(key))

                if current is None:
                    previous = None
                    row[f"{measure}_growth_pct"] = None
                    row[f"{measure}_trend"] = "NO_DATA"
                    continue

                if previous in (None, 0):
                    row[f"{measure}_growth_pct"] = None
                    row[f"{measure}_trend"] = "BASELINE"
                else:
                    growth = (
                        (current - previous)
                        / abs(previous)
                    ) * 100

                    row[f"{measure}_growth_pct"] = growth

                    if growth > 0:
                        row[f"{measure}_trend"] = "UP"
                    elif growth < 0:
                        row[f"{measure}_trend"] = "DOWN"
                    else:
                        row[f"{measure}_trend"] = "FLAT"

                previous = current

    def year_over_year(self, rows, measures):
        """
        Calculate YoY growth when a year field is present.
        Expects rows containing a period/year and a metric sum.
        """
        if not rows:
            return

        for measure in measures:
            key = f"{measure}_sum"

            if key not in rows[0]:
                continue

            previous_by_period = {}

            for row in rows:
                period = row.get("period")
                current = self._to_number(row.get(key))

                if not period or current is None:
                    continue

                try:
                    year = int(str(period)[:4])
                    month = int(str(period)[5:7])
                except (ValueError, TypeError):
                    continue

                previous_period = f"{year - 1:04d}-{month:02d}"

                if previous_period in previous_by_period:
                    previous = previous_by_period[previous_period]

                    if previous not in (None, 0):
                        row[f"{measure}_yoy_growth_pct"] = (
                            (current - previous)
                            / abs(previous)
                        ) * 100

                previous_by_period[period] = current

    def detect_trends(self, rows, measures):
        """Summarize the direction of each time-series metric."""
        trends = {}

        for measure in measures:
            key = f"{measure}_sum"

            growth = [
                self._to_number(row.get(f"{measure}_growth_pct"))
                for row in rows
                if row.get(f"{measure}_growth_pct") is not None
            ]

            growth = [x for x in growth if x is not None]

            if not growth:
                trends[measure] = "NO_DATA"
                continue

            positive = sum(1 for x in growth if x > 0)
            negative = sum(1 for x in growth if x < 0)

            if positive > negative:
                trends[measure] = "UPWARD"
            elif negative > positive:
                trends[measure] = "DOWNWARD"
            else:
                trends[measure] = "MIXED"

        return trends

    @staticmethod
    def _detect_date_column(data):
        if not data:
            return None

        columns = list(data[0].keys())

        preferred = [
            "date",
            "datetime",
            "timestamp",
            "created_at",
            "updated_at",
            "transaction_date",
            "order_date",
            "event_date",
            "sale_date",
            "invoice_date",
        ]

        lower_map = {
            column.lower(): column
            for column in columns
        }

        for name in preferred:
            if name in lower_map:
                return lower_map[name]

        # Conservative fallback: only names strongly suggesting a date.
        for column in columns:
            name = column.lower()

            if (
                name.endswith("_date")
                or name.endswith("_datetime")
                or name.endswith("_timestamp")
            ):
                return column

        return None

    @staticmethod
    def _parse_date(value):
        if value in (None, ""):
            return None

        if isinstance(value, datetime):
            return value

        value = str(value).strip()

        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    # ======================================================
    # KPI ENGINE
    # ======================================================

    def add_kpis(self, rows, measures):
        if not rows:
            return

        for measure in measures:
            sum_key = f"{measure}_sum"
            avg_key = f"{measure}_avg"

            if sum_key not in rows[0]:
                continue

            totals = [
                self._to_number(row.get(sum_key))
                for row in rows
            ]
            totals = [x for x in totals if x is not None]

            if not totals:
                continue

            total = sum(totals)

            for row in rows:
                value = self._to_number(row.get(sum_key))

                if value is None:
                    continue

                # KPI: contribution to total.
                row[f"{measure}_share_pct"] = (
                    (value / total) * 100
                    if total != 0
                    else 0
                )

                # Simple business label.
                average = self._to_number(row.get(avg_key))

                if average is not None and average > 0:
                    row[f"{measure}_performance"] = (
                        "HIGH" if value >= total / len(rows)
                        else "LOW"
                    )

    # ======================================================
    # COUNT DISTINCT
    # ======================================================

    @staticmethod
    def count_distinct(rows, column):
        return len({
            str(row.get(column))
            for row in rows
            if row.get(column) not in (None, "")
        })

    # ======================================================
    # RANKING
    # ======================================================

    def add_rankings(self, rows, measures):
        for measure in measures:
            key = f"{measure}_sum"

            if key not in rows[0]:
                continue

            ranked = sorted(
                rows,
                key=lambda row: (
                    self._to_number(row.get(key)) or 0
                ),
                reverse=True,
            )

            for index, row in enumerate(ranked, start=1):
                row[f"{measure}_rank"] = index

                if index <= min(10, len(ranked)):
                    row[f"{measure}_top_10"] = True
                else:
                    row[f"{measure}_top_10"] = False

                row[f"{measure}_bottom_10"] = (
                    index > max(0, len(ranked) - 10)
                )

    # ======================================================
    # ANOMALY FLAGS
    # ======================================================

    def add_anomaly_flags(self, rows, measures):
        for measure in measures:
            avg_key = f"{measure}_avg"
            if avg_key not in rows[0]:
                continue

            values = [
                self._to_number(row.get(avg_key))
                for row in rows
            ]
            values = [x for x in values if x is not None]

            if len(values) < 3:
                continue

            mean = sum(values) / len(values)
            std = self._stddev(values)

            for row in rows:
                value = self._to_number(row.get(avg_key))

                if value is None or std == 0:
                    row[f"{measure}_anomaly"] = False
                    continue

                z_score = abs((value - mean) / std)
                row[f"{measure}_z_score"] = z_score
                row[f"{measure}_anomaly"] = z_score >= 2.0

    # ======================================================
    # PERCENTAGE SHARE
    # ======================================================

    def add_percentage_share(self, rows, measures):
        # add_kpis() already provides share_pct.
        # This method remains explicit as a Gold capability.
        for measure in measures:
            key = f"{measure}_sum"

            if not rows or key not in rows[0]:
                continue

            total = sum(
                self._to_number(row.get(key)) or 0
                for row in rows
            )

            for row in rows:
                value = self._to_number(row.get(key)) or 0

                row[f"{measure}_percentage"] = (
                    (value / total) * 100
                    if total
                    else 0
                )

    # ======================================================
    # GOLD QUALITY
    # ======================================================

    def gold_quality(self, rows):
        checks = {
            "row_count": len(rows),
            "empty_rows": 0,
            "null_dimension_rows": 0,
            "duplicate_rows": 0,
            "negative_metric_values": 0,
        }

        if not rows:
            return {
                "status": "PASS",
                "score": 100.0,
                "checks": checks,
            }

        seen = set()

        for row in rows:
            normalized = tuple(
                sorted(
                    (str(key), str(value))
                    for key, value in row.items()
                )
            )

            if normalized in seen:
                checks["duplicate_rows"] += 1

            seen.add(normalized)

            if all(
                value in (None, "")
                for value in row.values()
            ):
                checks["empty_rows"] += 1

            for key, value in row.items():
                if (
                    key.endswith("_sum")
                    or key.endswith("_avg")
                ):
                    number = self._to_number(value)
                    if number is not None and number < 0:
                        checks["negative_metric_values"] += 1

        failed = 0

        if checks["empty_rows"]:
            failed += 1
        if checks["duplicate_rows"]:
            failed += 1
        if checks["negative_metric_values"]:
            failed += 1

        score = max(
            0.0,
            100.0 - (failed * 20.0),
        )

        return {
            "status": "PASS" if score == 100.0 else "WARN",
            "score": score,
            "checks": checks,
        }

    # ======================================================
    # RECONCILIATION
    # ======================================================

    @staticmethod
    def reconcile(
        source_rows,
        detail_rows,
        analytics_rows,
    ):
        detail_match = source_rows == detail_rows

        # Analytics is aggregated, so its row count does not
        # need to equal source rows.
        status = "PASS" if detail_match else "FAIL"

        return {
            "status": status,
            "source_rows": source_rows,
            "detail_rows": detail_rows,
            "analytics_rows": analytics_rows,
            "detail_row_difference": (
                detail_rows - source_rows
            ),
        }

    # ======================================================
    # AI-FRIENDLY BUSINESS INSIGHTS
    # ======================================================

    def generate_insights(
        self,
        rows,
        dimensions,
        measures,
    ):
        insights = []

        if not rows:
            return insights

        for measure in measures:
            key = f"{measure}_sum"

            if key not in rows[0]:
                continue

            ranked = sorted(
                rows,
                key=lambda row: (
                    self._to_number(row.get(key)) or 0
                ),
                reverse=True,
            )

            if not ranked:
                continue

            top = ranked[0]
            value = self._to_number(top.get(key))

            context = ", ".join(
                f"{dimension}={top.get(dimension)}"
                for dimension in dimensions
                if top.get(dimension) not in (None, "")
            )

            if context:
                insights.append(
                    f"{context} has the highest "
                    f"{measure}_sum ({value})."
                )

        return insights

    # ======================================================
    # FILE IO
    # ======================================================

    @staticmethod
    def _read_csv(path):
        rows = []

        with open(
            path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.DictReader(file)
            for row in reader:
                rows.append(dict(row))

        return rows

    @staticmethod
    def save(data, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if not data:
            path.write_text("", encoding="utf-8")
            return

        columns = list(data[0].keys())

        with open(
            path,
            "w",
            encoding="utf-8",
            newline="",
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=columns,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(data)

    # ======================================================
    # NUMERIC / STATISTICS
    # ======================================================

    @staticmethod
    def _to_number(value):
        if value is None:
            return None

        if isinstance(value, bool):
            return int(value)

        if isinstance(value, (int, float)):
            return value

        try:
            value = str(value).strip()

            if value == "":
                return None

            if "." in value:
                return float(value)

            return int(value)

        except (ValueError, TypeError):
            return None

    @staticmethod
    def _median(values):
        values = sorted(values)

        if not values:
            return None

        middle = len(values) // 2

        if len(values) % 2:
            return values[middle]

        return (
            values[middle - 1] +
            values[middle]
        ) / 2

    @staticmethod
    def _stddev(values):
        if len(values) <= 1:
            return 0.0

        mean = sum(values) / len(values)

        variance = sum(
            (value - mean) ** 2
            for value in values
        ) / len(values)

        return variance ** 0.5

    @staticmethod
    def _percentile(values, percentile):
        values = sorted(values)

        if not values:
            return None

        if len(values) == 1:
            return values[0]

        position = (
            (len(values) - 1)
            * (percentile / 100)
        )

        lower = int(position)
        upper = min(
            lower + 1,
            len(values) - 1,
        )

        weight = position - lower

        return (
            values[lower]
            + (values[upper] - values[lower])
            * weight
        )

    # ======================================================
    # FALLBACK SCHEMA
    # ======================================================

    def _infer_schema(self, data):
        if not data:
            return {"columns": []}

        columns = list(data[0].keys())
        schema = []

        for column in columns:
            values = [
                row.get(column)
                for row in data
                if row.get(column) not in (None, "")
            ]

            detected_type = self._detect_type(values)

            schema.append({
                "name": column,
                "type": detected_type,
                "nullable": any(
                    row.get(column) in (None, "")
                    for row in data
                ),
            })

        return {"columns": schema}

    @staticmethod
    def _detect_type(values):
        if not values:
            return "string"

        if all(
            str(value).isdigit()
            for value in values
        ):
            return "integer"

        try:
            for value in values:
                float(value)
            return "float"
        except (ValueError, TypeError):
            return "string"

