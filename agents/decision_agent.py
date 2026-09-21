class DecisionAgent:

    def analyze(self, data, result):

        print(
            "DECISION AGENT: "
            "Analyzing generic analytics output..."
        )

        if not result:

            print(
                "DECISION AGENT: "
                "No analytics result available."
            )

            return []

        analysis = []

        # ====================================================
        # DETECT DIMENSIONS
        # ====================================================

        for row in result:

            dimensions = {}
            metrics = {}

            for key, value in row.items():

                # Dimension columns
                if not any(
                    key.endswith(suffix)
                    for suffix in [
                        "_sum",
                        "_avg",
                        "_min",
                        "_max"
                    ]
                ) and key != "row_count":

                    dimensions[key] = value

                # Numeric analytics
                elif key != "row_count":

                    metrics[key] = value

            # =================================================
            # FIND AVERAGE METRICS
            # =================================================

            average_metrics = {
                key: value
                for key, value in metrics.items()
                if key.endswith("_avg")
            }

            # =================================================
            # DECISION LOGIC
            # =================================================

            decisions = []
            actions = []

            for metric_name, metric_value in average_metrics.items():

                try:

                    numeric_value = float(
                        metric_value
                    )

                except (
                    ValueError,
                    TypeError
                ):

                    continue

                base_name = metric_name[:-4]

                # ---------------------------------------------
                # Generic threshold
                # ---------------------------------------------

                if numeric_value >= 60000:

                    decision = (
                        f"HIGH {base_name.upper()}"
                    )

                    action = (
                        f"Monitor {base_name} trend"
                    )

                else:

                    decision = (
                        f"NORMAL {base_name.upper()}"
                    )

                    action = (
                        f"No immediate action for "
                        f"{base_name}"
                    )

                decisions.append(
                    decision
                )

                actions.append(
                    action
                )

            # =================================================
            # NO AVERAGE METRIC
            # =================================================

            if not average_metrics:

                decision = "ANALYTICS GENERATED"

                action = (
                    "Review generated analytics"
                )

            else:

                decision = " | ".join(
                    decisions
                )

                action = " | ".join(
                    actions
                )

            # =================================================
            # BUILD RESULT
            # =================================================

            output = {}

            output.update(
                dimensions
            )

            output["row_count"] = row.get(
                "row_count",
                0
            )

            output["metrics"] = metrics

            output["decision"] = decision

            output["action"] = action

            analysis.append(
                output
            )

        print(
            f"DECISION AGENT: "
            f"{len(analysis)} decisions generated"
        )

        return analysis


    # ========================================================
    # REPORT
    # ========================================================

    def show_report(self, analysis):

        print()
        print("=" * 70)
        print(
            "             DECISION REPORT"
        )
        print("=" * 70)

        if not analysis:

            print(
                "No decisions generated."
            )

            return

        for item in analysis:

            print()

            print("Dimensions:")

            for key, value in item.items():

                if key in {
                    "row_count",
                    "metrics",
                    "decision",
                    "action"
                }:

                    continue

                print(
                    f"  {key}: {value}"
                )

            print(
                f"Row Count: "
                f"{item['row_count']}"
            )

            print()
            print("Metrics:")

            metrics = item.get(
                "metrics",
                {}
            )

            if metrics:

                for key, value in metrics.items():

                    print(
                        f"  {key}: {value}"
                    )

            else:

                print(
                    "  No numeric metrics"
                )

            print()

            print(
                f"Decision: "
                f"{item['decision']}"
            )

            print(
                f"Action: "
                f"{item['action']}"
            )

            print(
                "-" * 70
            )


# ============================================================
# DIRECT TEST
# ============================================================

if __name__ == "__main__":

    agent = DecisionAgent()

    sample_result = [
        {
            "city": "Chennai",
            "row_count": 2,
            "salary_sum": 100000,
            "salary_avg": 50000,
            "salary_min": 40000,
            "salary_max": 60000
        },
        {
            "city": "Bangalore",
            "row_count": 3,
            "salary_sum": 210000,
            "salary_avg": 70000,
            "salary_min": 60000,
            "salary_max": 80000
        }
    ]

    analysis = agent.analyze(
        None,
        sample_result
    )

    agent.show_report(
        analysis
    )