import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest import IngestionManifest, _is_excluded_dir, _is_excluded_file


class ManifestExclusionTests(unittest.TestCase):
    def manifest(self, *paths: str) -> IngestionManifest:
        return IngestionManifest(
            project_name="test",
            exclude_paths=frozenset(paths),
        )

    def test_exact_relative_file_path_is_excluded(self) -> None:
        manifest = self.manifest("k8s/base/mongodb-k8s.yaml")

        self.assertTrue(
            _is_excluded_file("mongodb-k8s.yaml", "k8s/base", manifest)
        )
        self.assertFalse(
            _is_excluded_file("mongodb-k8s.yaml", "k8s/other", manifest)
        )

    def test_leaf_file_name_is_excluded_at_any_depth(self) -> None:
        manifest = self.manifest("development-values.yaml")

        self.assertTrue(
            _is_excluded_file("development-values.yaml", "config/local", manifest)
        )

    def test_leaf_directory_name_is_excluded_at_any_depth(self) -> None:
        manifest = self.manifest("generated")

        self.assertTrue(_is_excluded_dir("generated", "docs", manifest))

    def test_relative_directory_path_is_scoped(self) -> None:
        manifest = self.manifest("docs/generated")

        self.assertTrue(_is_excluded_dir("generated", "docs", manifest))
        self.assertFalse(_is_excluded_dir("generated", "other", manifest))


if __name__ == "__main__":
    unittest.main()
