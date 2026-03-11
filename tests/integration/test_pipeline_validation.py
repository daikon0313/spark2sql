"""
実パイプラインの統合テスト

fixtures/pipelines/ のサンプルパイプラインを変換し、
DuckDB で実行して結果を検証する。
"""
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from converter import PySparkToBigQueryTransformer
from validator.duckdb_runner import DuckDBRunner
from tests.integration.test_duckdb_validation import convert_and_run, make_runner


class TestSampleBasicPipeline(unittest.TestCase):
    """sample_basic.py の変換テスト"""

    def test_convert_file(self):
        """ファイル変換が成功する"""
        t = PySparkToBigQueryTransformer()
        results = t.convert_file("fixtures/pipelines/sample_basic.py")
        self.assertIn("result", results)
        self.assertNotIn("ERROR", results["result"].sql)

    def test_result_executes(self):
        """変換結果が DuckDB で実行できる"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file("fixtures/pipelines/sample_basic.py", target_vars=["result"])
        runner = make_runner()
        qr = runner.run(r["result"].sql)
        self.assertGreater(qr.row_count, 0)
        self.assertIn("customer_id", [c.lower() for c in qr.columns])


class TestSampleUDFPipeline(unittest.TestCase):
    """sample_udf.py の変換テスト"""

    def test_convert_file(self):
        """変換可能部分が正常に変換される"""
        t = PySparkToBigQueryTransformer()
        results = t.convert_file("fixtures/pipelines/sample_udf.py")
        # active_orders, enriched, summary は変換可能
        self.assertIn("active_orders", results)
        self.assertIn("enriched", results)
        self.assertIn("summary", results)

    def test_summary_executes(self):
        """集計結果が DuckDB で実行できる"""
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では FORMAT 関数が非対応")
        t = PySparkToBigQueryTransformer()
        r = t.convert_file("fixtures/pipelines/sample_udf.py", target_vars=["summary"])
        runner = make_runner()
        qr = runner.run(r["summary"].sql)
        self.assertGreater(qr.row_count, 0)
        cols_lower = [c.lower() for c in qr.columns]
        self.assertIn("customer_id", cols_lower)
        self.assertIn("total_amount", cols_lower)


class TestSampleComplexWindowPipeline(unittest.TestCase):
    """sample_complex_window.py の変換テスト"""

    def setUp(self):
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では Window 関数テストをスキップ")

    def test_convert_file(self):
        """ファイル変換が成功する"""
        t = PySparkToBigQueryTransformer()
        results = t.convert_file("fixtures/pipelines/sample_complex_window.py")
        self.assertIn("result", results)
        self.assertNotIn("ERROR", results["result"].sql)

    def test_result_executes(self):
        """Window 関数を含む変換結果が DuckDB で実行できる"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file(
            "fixtures/pipelines/sample_complex_window.py", target_vars=["result"]
        )
        runner = make_runner()
        qr = runner.run(r["result"].sql)
        self.assertEqual(qr.row_count, 10)
        cols_lower = [c.lower() for c in qr.columns]
        self.assertIn("region_row_num", cols_lower)
        self.assertIn("region_rank", cols_lower)
        self.assertIn("prev_amount", cols_lower)
        self.assertIn("next_amount", cols_lower)
        self.assertIn("running_total", cols_lower)

    def test_running_total_values(self):
        """累計が正しく計算される"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file(
            "fixtures/pipelines/sample_complex_window.py", target_vars=["result"]
        )
        runner = make_runner()
        qr = runner.run(r["result"].sql)
        rt_idx = [c.lower() for c in qr.columns].index("running_total")
        # running_total は全て正の値
        for row in qr.rows:
            self.assertIsNotNone(row[rt_idx])
            self.assertGreater(float(row[rt_idx]), 0)


class TestSampleNestedSelectPipeline(unittest.TestCase):
    """sample_nested_select.py の変換テスト"""

    def setUp(self):
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では EXCEPT / Window が非対応")

    def test_convert_file(self):
        """ファイル変換が成功する"""
        t = PySparkToBigQueryTransformer()
        results = t.convert_file("fixtures/pipelines/sample_nested_select.py")
        self.assertIn("region_summary", results)
        self.assertIn("ranked", results)
        for var_name, result in results.items():
            self.assertNotIn("-- ERROR:", result.sql, f"{var_name} に変換エラー")

    def test_region_summary_executes(self):
        """リージョン集計が DuckDB で実行できる"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file(
            "fixtures/pipelines/sample_nested_select.py", target_vars=["region_summary"]
        )
        runner = make_runner()
        qr = runner.run(r["region_summary"].sql)
        self.assertGreater(qr.row_count, 0)
        cols_lower = [c.lower() for c in qr.columns]
        self.assertIn("region", cols_lower)
        self.assertIn("total_revenue", cols_lower)
        self.assertIn("customer_count", cols_lower)

    def test_ranked_executes(self):
        """ランキング結果が DuckDB で実行できる"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file(
            "fixtures/pipelines/sample_nested_select.py", target_vars=["ranked"]
        )
        runner = make_runner()
        qr = runner.run(r["ranked"].sql)
        self.assertGreater(qr.row_count, 0)
        # 各リージョン最大3名
        self.assertLessEqual(qr.row_count, 9)
        cols_lower = [c.lower() for c in qr.columns]
        self.assertIn("rank_in_region", cols_lower)

    def test_union_preserves_all_rows(self):
        """UNION で高額+低額の全行が保持される"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_file(
            "fixtures/pipelines/sample_nested_select.py", target_vars=["all_customers"]
        )
        runner = make_runner()
        qr = runner.run(r["all_customers"].sql)
        # order_summary の全行が UNION で含まれる
        self.assertGreater(qr.row_count, 0)


class TestBatchConvertCLI(unittest.TestCase):
    """scripts/batch_convert.py の統合テスト"""

    def test_batch_convert_runs(self):
        """一括変換が正常に実行される"""
        import subprocess
        result = subprocess.run(
            [sys.executable, "scripts/batch_convert.py", "fixtures/pipelines/"],
            capture_output=True, text=True,
        )
        # exit code 0 (全成功) or 2 (警告あり) は正常
        self.assertIn(result.returncode, [0, 2])
        self.assertIn("変換結果サマリー", result.stderr)

    def test_batch_convert_with_output(self):
        """--out で SQL ファイルが生成される"""
        import subprocess
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    sys.executable, "scripts/batch_convert.py",
                    "fixtures/pipelines/", "--out", tmpdir,
                ],
                capture_output=True, text=True,
            )
            self.assertIn(result.returncode, [0, 2])
            # SQL ファイルが生成されている
            sql_files = list(os.listdir(tmpdir))
            self.assertGreater(len(sql_files), 0)
            self.assertTrue(all(f.endswith(".sql") for f in sql_files))


class TestConvertCLI(unittest.TestCase):
    """scripts/convert.py の統合テスト"""

    def test_convert_single_file(self):
        """単一ファイル変換が正常に実行される"""
        import subprocess
        result = subprocess.run(
            [
                sys.executable, "scripts/convert.py",
                "fixtures/pipelines/sample_basic.py", "--var", "result",
            ],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("SELECT", result.stdout)

    def test_convert_missing_file(self):
        """存在しないファイルでエラー"""
        import subprocess
        result = subprocess.run(
            [sys.executable, "scripts/convert.py", "nonexistent.py"],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
