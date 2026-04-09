import logging
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.logger import configure_entrypoint_logging


class TestLoggerSetup(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="logger-setup-"))
        self.root_logger = logging.getLogger()
        self.original_handlers = list(self.root_logger.handlers)
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)
            handler.close()

    def tearDown(self):
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)
            handler.close()

        for handler in self.original_handlers:
            self.root_logger.addHandler(handler)

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_configure_entrypoint_logging_reuses_existing_handlers(self):
        first_log_file = configure_entrypoint_logging(self.temp_dir)
        logging.getLogger("qa.logger").info("first message")

        second_log_file = configure_entrypoint_logging(self.temp_dir)
        logging.getLogger("qa.logger").info("second message")

        log_files = sorted((self.temp_dir / "logs").glob("*.log"))
        self.assertTrue(first_log_file)
        self.assertEqual(second_log_file, "")
        self.assertEqual(len(log_files), 1)

        contents = log_files[0].read_text(encoding="utf-8")
        self.assertIn("first message", contents)
        self.assertIn("second message", contents)


if __name__ == "__main__":
    unittest.main()
