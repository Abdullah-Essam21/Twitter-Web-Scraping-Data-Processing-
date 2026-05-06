import os
import shutil
import json
import logging
from src.utils.helpers import sanitize_folder_name, format_timestamp

logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self, base_data_dir="data"):
        self.base_dir = base_data_dir
        self.raw_dir = os.path.join(base_data_dir, "raw")
        self.processed_dir = os.path.join(base_data_dir, "processed")
        self._ensure_dirs()

    def _ensure_dirs(self):
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def create_session_dir(self, session_name, prefix=""):
        slug = sanitize_folder_name(session_name)
        timestamp = format_timestamp()
        folder_name = f"{prefix}{slug}_{timestamp}"
        path = os.path.join(self.raw_dir, folder_name)
        os.makedirs(path, exist_ok=True)
        return path

    def save_html(self, directory, page_number, content):
        filename = f"page_{page_number}.html"
        path = os.path.join(directory, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def save_json(self, data, filename, directory=None):
        target_dir = directory or self.processed_dir
        path = os.path.join(target_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return path

    def clear_raw_data(self):
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)
            os.makedirs(self.raw_dir)
            logger.info("Cleared raw data directory.")
