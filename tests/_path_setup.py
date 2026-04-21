import sys
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR_STR = str(TESTS_DIR)
ROOT_STR = str(ROOT)

if TESTS_DIR_STR not in sys.path:
    sys.path.insert(0, TESTS_DIR_STR)

if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)
