"""Unit tests for filesystem utilities."""
import json
import pytest
from pathlib import Path
from src.fs.utils import slugify, sanitize_filename, atomic_write_jsonl, ensure_directory


class TestSlugify:
    """Tests for slugify function."""

    def test_slugify_basic(self):
        """Test basic character removal."""
        assert slugify('Hello: "World"?*') == 'Hello World'

    def test_slugify_empty_string(self):
        """Test empty string returns 'untitled'."""
        assert slugify('') == 'untitled'
        assert slugify('   ') == 'untitled'

    def test_slugify_removes_newlines(self):
        """Test newlines are replaced with spaces."""
        assert slugify('Line1\nLine2\rLine3') == 'Line1 Line2 Line3'

    def test_slugify_collapses_spaces(self):
        """Test multiple spaces are collapsed to single space."""
        assert slugify('Hello    World') == 'Hello World'

    def test_slugify_max_length(self):
        """Test length limiting."""
        long_text = 'a' * 200
        result = slugify(long_text, max_length=50)
        assert len(result) == 50

    def test_slugify_reserved_names(self):
        """Test Windows reserved names are prefixed with underscore."""
        assert slugify('CON') == '_CON'
        assert slugify('con') == '_con'
        assert slugify('PRN') == '_PRN'
        assert slugify('COM1') == '_COM1'

    def test_slugify_trailing_dots(self):
        """Test trailing dots and spaces are removed."""
        assert slugify('filename...') == 'filename'
        assert slugify('filename   ') == 'filename'
        assert slugify('filename. . .') == 'filename'


class TestSanitizeFilename:
    """Tests for sanitize_filename function."""

    def test_sanitize_filename_reserved_name(self):
        """Test reserved names with extension."""
        assert sanitize_filename('con.txt') == '_con.txt'
        assert sanitize_filename('PRN.log') == '_PRN.log'

    def test_sanitize_filename_empty(self):
        """Test empty filename."""
        assert sanitize_filename('') == 'unnamed'

    def test_sanitize_filename_with_extension(self):
        """Test filename with extension is preserved."""
        result = sanitize_filename('test: file?.jpg')
        assert result == 'test file.jpg'

    def test_sanitize_filename_removes_invalid_chars(self):
        """Test invalid characters are removed."""
        result = sanitize_filename('file<>:|?.txt')
        assert '<' not in result
        assert '>' not in result
        assert ':' not in result
        assert '|' not in result
        assert '?' not in result


class TestEnsureDirectory:
    """Tests for ensure_directory function."""

    def test_ensure_directory_creates_dir(self, tmp_path):
        """Test directory is created."""
        test_dir = tmp_path / 'test_subdir'
        result = ensure_directory(test_dir)
        assert result.exists()
        assert result.is_dir()

    def test_ensure_directory_nested(self, tmp_path):
        """Test nested directory creation."""
        nested_dir = tmp_path / 'level1' / 'level2' / 'level3'
        result = ensure_directory(nested_dir)
        assert result.exists()
        assert result.is_dir()

    def test_ensure_directory_existing(self, tmp_path):
        """Test existing directory doesn't raise error."""
        test_dir = tmp_path / 'existing'
        test_dir.mkdir()
        result = ensure_directory(test_dir)
        assert result.exists()


class TestAtomicWriteJsonl:
    """Tests for atomic_write_jsonl function."""

    def test_atomic_write_jsonl_basic(self, tmp_path):
        """Test basic JSONL writing."""
        test_file = tmp_path / 'test.jsonl'
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]

        atomic_write_jsonl(test_file, test_data)

        assert test_file.exists()

        # Verify content
        lines = test_file.read_text(encoding='utf-8').strip().split('\n')
        assert len(lines) == 2
        assert json.loads(lines[0]) == {'id': 1, 'name': 'Alice'}
        assert json.loads(lines[1]) == {'id': 2, 'name': 'Bob'}

    def test_atomic_write_jsonl_empty_list(self, tmp_path):
        """Test writing empty list."""
        test_file = tmp_path / 'empty.jsonl'
        atomic_write_jsonl(test_file, [])
        assert test_file.exists()
        assert test_file.read_text() == ''

    def test_atomic_write_jsonl_unicode(self, tmp_path):
        """Test writing Unicode characters."""
        test_file = tmp_path / 'unicode.jsonl'
        test_data = [
            {'text': 'Привет мир'},
            {'text': '你好世界'}
        ]

        atomic_write_jsonl(test_file, test_data)

        lines = test_file.read_text(encoding='utf-8').strip().split('\n')
        assert json.loads(lines[0]) == {'text': 'Привет мир'}
        assert json.loads(lines[1]) == {'text': '你好世界'}
