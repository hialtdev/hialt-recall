import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import rag_engine


class ConnectionTests(unittest.TestCase):
    def test_autocommit_is_set_before_vector_registration(self) -> None:
        connection = Mock()
        settings = Mock()

        def assert_autocommit_is_ready(candidate: Mock) -> None:
            self.assertIs(candidate, connection)
            self.assertTrue(candidate.autocommit)

        with (
            patch.object(rag_engine, "_connect_raw", return_value=connection),
            patch.object(
                rag_engine,
                "register_vector",
                side_effect=assert_autocommit_is_ready,
            ) as register_vector,
        ):
            result = rag_engine.get_connection(settings, autocommit=True)

        self.assertIs(result, connection)
        self.assertTrue(connection.autocommit)
        register_vector.assert_called_once_with(connection)


if __name__ == "__main__":
    unittest.main()
