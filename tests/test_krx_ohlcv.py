import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.krx_ohlcv import KrxOHLCVReader


class TestKrxOHLCV(unittest.TestCase):
    def test_login_krx_success(self):
        session = MagicMock()
        session.post.return_value.json.return_value = {"_error_code": "CD001"}

        reader = KrxOHLCVReader(session=session, login_id="id", login_pw="pw")
        try:
            self.assertTrue(reader.login_krx())
        finally:
            reader.close()

        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(session.post.call_count, 1)

    def test_login_krx_retries_duplicate_login(self):
        session = MagicMock()
        session.post.side_effect = [
            MagicMock(json=MagicMock(return_value={"_error_code": "CD011"})),
            MagicMock(json=MagicMock(return_value={"_error_code": "CD001"})),
        ]

        reader = KrxOHLCVReader(session=session, login_id="id", login_pw="pw")
        try:
            self.assertTrue(reader.login_krx())
        finally:
            reader.close()

        self.assertEqual(session.post.call_count, 2)
        retry_payload = session.post.call_args_list[1].kwargs["data"]
        self.assertEqual(retry_payload["skipDup"], "Y")

    def test_loads_credentials_from_file(self):
        session = MagicMock()
        with tempfile.TemporaryDirectory() as temp_dir:
            credential_path = Path(temp_dir) / "krx_marketplace.json"
            credential_path.write_text('{"login_id": "file_id", "login_pw": "file_pw"}', encoding="utf-8")
            reader = KrxOHLCVReader(session=session, credentials_path=credential_path)

        try:
            self.assertEqual(reader._login_id, "file_id")
            self.assertEqual(reader._login_pw, "file_pw")
        finally:
            reader.close()


if __name__ == "__main__":
    unittest.main()
