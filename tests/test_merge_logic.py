import sys
import types
import unittest

import pandas as pd

if "flask" not in sys.modules:
    fake_flask = types.ModuleType("flask")

    class _FakeFlask:
        def __init__(self, *args, **kwargs):
            self.config = {}

        def route(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    fake_flask.Flask = _FakeFlask
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = types.SimpleNamespace(files=None, form=None, method="GET")
    fake_flask.send_file = lambda *args, **kwargs: None
    fake_flask.send_from_directory = lambda *args, **kwargs: None
    sys.modules["flask"] = fake_flask

if "openpyxl" not in sys.modules:
    fake_openpyxl = types.ModuleType("openpyxl")
    fake_styles = types.ModuleType("openpyxl.styles")

    fake_openpyxl.load_workbook = lambda *args, **kwargs: None
    fake_styles.PatternFill = lambda *args, **kwargs: None

    sys.modules["openpyxl"] = fake_openpyxl
    sys.modules["openpyxl.styles"] = fake_styles

from app import (
    build_plan_groups,
    evaluate_primary_warning,
    evaluate_secondary_dau_warning,
    exclude_alert_groups_from_plan,
    filter_successful_swap_logs,
    merge_output_rows_by_target,
    regroup_for_requested_pair,
)


class MergeLogicTests(unittest.TestCase):
    def test_build_plan_groups_merges_same_logical_group(self):
        rows = [
            {"row_idx": 2, "target": 1001, "participants": [1002]},
            {"row_idx": 3, "target": 1001, "participants": [1003]},
            {"row_idx": 4, "target": 2001, "participants": [2002]},
        ]

        groups = build_plan_groups(rows)

        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0]["target"], 1001)
        self.assertEqual(groups[0]["members"], [1001, 1002, 1003])
        self.assertEqual(groups[0]["row_indices"], [2, 3])
        self.assertEqual(groups[1]["target"], 2001)
        self.assertEqual(groups[1]["members"], [2001, 2002])

    def test_regroup_for_requested_pair_extracts_pair_and_leftovers(self):
        groups = [
            {"target": 11, "members": [11, 33], "row_indices": [2], "anchor_row": 2},
            {"target": 44, "members": [22, 44, 55], "row_indices": [3, 4], "anchor_row": 3},
            {"target": 80, "members": [80, 81], "row_indices": [5], "anchor_row": 5},
        ]

        regrouped_groups, change = regroup_for_requested_pair(groups, 11, 22)

        self.assertEqual(
            regrouped_groups,
            [
                {"target": 11, "members": [11, 22], "row_indices": [2], "anchor_row": 2},
                {"target": 33, "members": [33, 44, 55], "row_indices": [3], "anchor_row": 3},
                {"target": 80, "members": [80, 81], "row_indices": [5], "anchor_row": 5},
            ],
        )
        self.assertEqual(change["requested_group"]["members"], [11, 22])
        self.assertEqual(change["leftover_group"]["members"], [33, 44, 55])
        self.assertEqual(change["source_rows"], [2, 3, 4])

    def test_regroup_for_requested_pair_handles_same_source_group(self):
        groups = [
            {"target": 10, "members": [10, 20, 30], "row_indices": [2, 3], "anchor_row": 2},
        ]

        regrouped_groups, change = regroup_for_requested_pair(groups, 10, 20)

        self.assertEqual(
            regrouped_groups,
            [
                {"target": 10, "members": [10, 20], "row_indices": [2], "anchor_row": 2},
                {"target": 30, "members": [30], "row_indices": [3], "anchor_row": 3},
            ],
        )
        self.assertEqual(change["leftover_group"]["members"], [30])

    def test_primary_and_secondary_warning_rules_follow_new_requirement(self):
        df = pd.DataFrame(
            [
                {"区服ID": 11, "真实排名": 1, "最高玩家累充金额": 8000, "前3名战力之和": 1_000_000_000, "DAU": 30},
                {"区服ID": 22, "真实排名": 3, "最高玩家累充金额": 9000, "前3名战力之和": 1_100_000_000, "DAU": 20},
                {"区服ID": 33, "真实排名": 10, "最高玩家累充金额": 500, "前3名战力之和": 200_000_000, "DAU": 4},
                {"区服ID": 44, "真实排名": 11, "最高玩家累充金额": 500, "前3名战力之和": 210_000_000, "DAU": 5},
            ]
        )

        primary = evaluate_primary_warning(df, 11, 22, total_servers=12)
        secondary = evaluate_secondary_dau_warning(df, [33, 44], primary["triggered"])

        self.assertTrue(primary["triggered"])
        self.assertIn("排名接近(差2)", primary["reasons"])
        self.assertIn("高战高充(前25%)", primary["reasons"])
        self.assertIn("战力接近(差<=5亿)", primary["reasons"])
        self.assertTrue(secondary["triggered"])
        self.assertEqual(secondary["low_dau_ids"], [33])
        self.assertIn("33 DAU<5(4)", secondary["reason"])

    def test_secondary_warning_skips_when_primary_not_triggered(self):
        df = pd.DataFrame(
            [
                {"区服ID": 11, "真实排名": 20, "最高玩家累充金额": 100, "前3名战力之和": 100, "DAU": 30},
                {"区服ID": 22, "真实排名": 50, "最高玩家累充金额": 100, "前3名战力之和": 9_000_000_000, "DAU": 30},
                {"区服ID": 33, "真实排名": 80, "最高玩家累充金额": 100, "前3名战力之和": 50, "DAU": 1},
            ]
        )

        primary = evaluate_primary_warning(df, 11, 22, total_servers=100)
        secondary = evaluate_secondary_dau_warning(df, [33], primary["triggered"])

        self.assertFalse(primary["triggered"])
        self.assertFalse(secondary["triggered"])
        self.assertEqual(secondary["low_dau_ids"], [])

    def test_exclude_primary_and_secondary_alert_groups_from_result(self):
        groups = [
            {"target": 61158, "members": [61158, 64182], "row_indices": [7], "anchor_row": 7},
            {"target": 64166, "members": [64166, 64193], "row_indices": [12], "anchor_row": 12},
            {"target": 70001, "members": [70001, 70002], "row_indices": [15], "anchor_row": 15},
        ]

        filtered_groups = exclude_alert_groups_from_plan(
            groups,
            primary_alert_groups=[{"ids": [61158, 64182], "reason": "排名接近(差5)"}],
            secondary_alert_groups=[{"ids": [64166, 64193], "reason": "剩余组存在低 DAU 区服"}],
        )

        self.assertEqual(
            filtered_groups,
            [
                {"target": 70001, "members": [70001, 70002], "row_indices": [15], "anchor_row": 15},
            ],
        )

    def test_filter_successful_swap_logs_excludes_alerted_requests(self):
        swapped_log_data = [
            {"合并申请": "1001+1002", "状态": "成功合并"},
            {"合并申请": "2001+2002", "状态": "常规预警已排除"},
            {"合并申请": "3001+3002", "状态": "二次预警已排除"},
            {"合并申请": "4001+4002", "状态": "预警已排除"},
        ]

        successful_logs = filter_successful_swap_logs(swapped_log_data)

        self.assertEqual(
            successful_logs,
            [
                {"合并申请": "1001+1002", "状态": "成功合并"},
            ],
        )

    def test_merge_output_rows_by_target_deduplicates_and_joins_participants(self):
        rows = [
            {"目标服": 3001, "参与服": "3002,3003"},
            {"目标服": 3001, "参与服": "3003,3004"},
            {"目标服": 4001, "参与服": "4002"},
        ]

        merged_rows = merge_output_rows_by_target(rows)

        self.assertEqual(
            merged_rows,
            [
                {"目标服": 3001, "参与服": "3002,3003,3004"},
                {"目标服": 4001, "参与服": "4002"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
