import unittest

import pandas as pd

from app import (
    build_plan_groups,
    evaluate_primary_warning,
    evaluate_secondary_dau_warning,
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
                {"区服ID": 11, "真实排名": 1, "最高玩家累充金额": 8000, "前2名战力之和": 1_000_000_000, "DAU": 30},
                {"区服ID": 22, "真实排名": 3, "最高玩家累充金额": 9000, "前2名战力之和": 1_100_000_000, "DAU": 20},
                {"区服ID": 33, "真实排名": 10, "最高玩家累充金额": 500, "前2名战力之和": 200_000_000, "DAU": 4},
                {"区服ID": 44, "真实排名": 11, "最高玩家累充金额": 500, "前2名战力之和": 210_000_000, "DAU": 5},
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
                {"区服ID": 11, "真实排名": 20, "最高玩家累充金额": 100, "前2名战力之和": 100, "DAU": 30},
                {"区服ID": 22, "真实排名": 50, "最高玩家累充金额": 100, "前2名战力之和": 9_000_000_000, "DAU": 30},
                {"区服ID": 33, "真实排名": 80, "最高玩家累充金额": 100, "前2名战力之和": 50, "DAU": 1},
            ]
        )

        primary = evaluate_primary_warning(df, 11, 22, total_servers=100)
        secondary = evaluate_secondary_dau_warning(df, [33], primary["triggered"])

        self.assertFalse(primary["triggered"])
        self.assertFalse(secondary["triggered"])
        self.assertEqual(secondary["low_dau_ids"], [])

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
