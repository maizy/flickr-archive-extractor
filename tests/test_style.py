import unittest
import os.path

import pycodestyle


class TestStyle(unittest.TestCase):
    CHECKED_PATHS = ('tests', 'flickr_archive_extractor.py', 'setup.py')
    ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)))

    def test_pycodestyle(self):
        style_guide = pycodestyle.StyleGuide(
            show_pep8=False,
            show_source=True,
            max_line_length=120,
        )
        result = style_guide.check_files([os.path.join(self.ROOT, p) for p in self.CHECKED_PATHS])
        self.assertEqual(result.total_errors, 0, 'Pycodestyle found code style errors or warnings')
