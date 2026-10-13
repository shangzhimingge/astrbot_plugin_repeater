from __future__ import annotations

import errno
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests._astrbot_stubs import install

LOGGER = install()

import main


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.official = self.root / "data" / "plugin_data" / "astrbot_plugin_repeater"
        self.module_path = self.root / "deployed" / "data" / "plugins" / "astrbot_plugin_repeater" / "main.py"
        self.cwd = self.root / "cwd"
        self.cwd.mkdir()
        LOGGER.warnings.clear()

    def tearDown(self):
        self.temp.cleanup()

    def make_plugin(self, *, load=True):
        with mock.patch.object(main.StarTools, "get_data_dir", return_value=self.official) as get_dir:
            plugin = object.__new__(main.RepeatPlugin)
            plugin.config = {"enable": True, "threshold": 3, "cooldown": 0}
            plugin._groups = {}
            plugin._state_load_blocked = False
            plugin._migration_pending = False
            plugin._state_file = plugin._data_dir() / "state.json"
            self.assertEqual(get_dir.call_args.args, ("astrbot_plugin_repeater",))
        if load:
            with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
                plugin._load_state()
        return plugin

    def legacy_paths(self, plugin):
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            return plugin._legacy_state_files()

    @staticmethod
    def write_state(path: Path, groups=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"groups": groups or {}}), encoding="utf-8")

    def state(self, plugin, gid="g"):
        return plugin._groups[gid]

    def test_official_path_and_plugin_name(self):
        plugin = self.make_plugin()
        self.assertEqual(plugin._state_file, self.official / "state.json")

    def test_official_valid_file_is_authoritative_over_legacy(self):
        plugin = self.make_plugin(load=False)
        self.write_state(plugin._state_file, {"official": {"enabled": False, "threshold": 8}})
        legacy = self.root / "data" / "astrbot_plugin_repeater" / "state.json"
        self.write_state(legacy, {"legacy": {"threshold": 11}})
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            plugin._load_state()
        self.assertEqual(set(plugin._groups), {"official"})
        self.assertFalse(self.state(plugin, "official").enabled)
        self.assertFalse(plugin._migration_pending)

    def test_corrupt_official_is_preserved_and_suppresses_saves(self):
        plugin = self.make_plugin(load=False)
        plugin._state_file.parent.mkdir(parents=True)
        plugin._state_file.write_bytes(b"{broken")
        original = plugin._state_file.read_bytes()
        legacy = self.legacy_paths(plugin)[0]
        self.write_state(legacy, {"legacy": {"threshold": 12}})
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            plugin._load_state()
        plugin._get_group("new")
        plugin._save_state()
        self.assertTrue(plugin._state_load_blocked)
        self.assertEqual(plugin._groups.keys(), {"new"})
        self.assertEqual(plugin._state_file.read_bytes(), original)
        self.assertEqual(json.loads(legacy.read_text(encoding="utf-8"))["groups"]["legacy"]["threshold"], 12)
        self.assertEqual(list(plugin._state_file.parent.glob("*.tmp")), [])

    def test_whitelist_order_deduplicates_and_excludes_official(self):
        plugin = self.make_plugin(load=False)
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.root)):
            candidates = plugin._legacy_state_files()
        self.assertEqual(candidates[0], self.root / "data" / "astrbot_plugin_repeater" / "state.json")
        self.assertEqual(len(candidates), len(set(candidates)))
        self.assertEqual(candidates.count(self.root / "data" / "astrbot_plugin_repeater" / "state.json"), 1)
        self.assertNotIn(plugin._state_file, candidates)
        self.assertEqual(candidates[-1], self.module_path.parent / "state.json")

    def test_module_parent_candidate_requires_complete_deployed_layout(self):
        plugin = self.make_plugin(load=False)
        not_deployed = self.root / "elsewhere" / "plugins" / "astrbot_plugin_repeater" / "main.py"
        with mock.patch.object(main, "__file__", str(not_deployed)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            candidates = plugin._legacy_state_files()
        self.assertNotIn(self.root / "elsewhere" / "astrbot_plugin_repeater" / "state.json", candidates)

    def test_each_legacy_fallback_source_migrates_without_modifying_source(self):
        plugin = self.make_plugin(load=False)
        candidates = self.legacy_paths(plugin)
        for index, source in enumerate(candidates):
            with self.subTest(source=source):
                plugin._state_file.unlink(missing_ok=True)
                if index:
                    for prior in candidates[:index]:
                        prior.unlink(missing_ok=True)
                self.write_state(source, {"g": {"threshold": index + 3}})
                original = source.read_bytes()
                plugin = self.make_plugin(load=False)
                with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
                    plugin._load_state()
                self.assertEqual(self.state(plugin).threshold, index + 3)
                self.assertTrue(plugin._state_file.exists())
                self.assertEqual(source.read_bytes(), original)

    def test_missing_legacy_sources_leave_empty_state(self):
        plugin = self.make_plugin()
        self.assertEqual(plugin._groups, {})
        self.assertFalse(plugin._state_file.exists())

    def test_valid_empty_snapshot_and_default_settings_migrate(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"empty-settings": {}})
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            plugin._load_state()
        self.assertEqual(self.state(plugin, "empty-settings").threshold, 3)
        plugin._state_file.unlink()
        self.write_state(source, {})
        empty = self.make_plugin(load=False)
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            empty._load_state()
        self.assertEqual(empty._groups, {})
        self.assertTrue(empty._state_file.exists())

    def test_all_groups_validate_before_memory_swap(self):
        plugin = self.make_plugin(load=False)
        plugin._groups = {"before": main._GroupState(True, 7, 1)}
        source = self.root / "state.json"
        for bad_value in (
            {"enabled": "false"},
            {"threshold": float("inf")},
            {"threshold": float("nan")},
            {"cooldown": float("nan")},
            {"cooldown": float("inf")},
        ):
            with self.subTest(bad_value=bad_value):
                self.write_state(source, {"valid": {"threshold": 8}, "bad": bad_value})
                with self.assertRaises((ValueError, OverflowError)):
                    plugin._parse_state_snapshot(source)
        self.assertEqual(set(plugin._groups), {"before"})

    def test_malformed_first_legacy_then_valid_next(self):
        plugin = self.make_plugin(load=False)
        candidates = self.legacy_paths(plugin)
        for malformed in ("[]", '{"groups":'):
            with self.subTest(malformed=malformed):
                plugin._state_file.unlink(missing_ok=True)
                for candidate in candidates:
                    candidate.unlink(missing_ok=True)
                candidates[0].parent.mkdir(parents=True, exist_ok=True)
                candidates[0].write_text(malformed, encoding="utf-8")
                self.write_state(candidates[1], {"second": {"threshold": 9}})
                plugin = self.make_plugin(load=False)
                with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
                    plugin._load_state()
                self.assertEqual(set(plugin._groups), {"second"})

    def test_symlinked_legacy_source_is_skipped(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"private": {"threshold": 12}})
        original = source.read_bytes()
        is_symlink = lambda path: path == source
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            with mock.patch.object(Path, "is_symlink", autospec=True, side_effect=is_symlink):
                plugin._load_state()
        self.assertNotIn("private", plugin._groups)
        self.assertFalse(plugin._state_file.exists())
        self.assertEqual(source.read_bytes(), original)

    def test_retry_migration_failure_preserves_memory_and_legacy(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"g": {"threshold": 4}})
        old_bytes = source.read_bytes()
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)), mock.patch.object(main.os, "link", side_effect=OSError(errno.EPERM, "blocked")):
            plugin._load_state()
        self.assertTrue(plugin._migration_pending)
        self.assertEqual(self.state(plugin).threshold, 4)
        self.state(plugin).threshold = 12
        with mock.patch.object(main.os, "link", wraps=os.link) as link:
            plugin._save_state()
        self.assertEqual(link.call_count, 1)
        self.assertEqual(json.loads(plugin._state_file.read_text(encoding="utf-8"))["groups"]["g"]["threshold"], 12)
        self.assertEqual(source.read_bytes(), old_bytes)
        self.assertEqual(list(self.official.glob("*.tmp")), [])

    def test_temp_creation_failure_keeps_memory_and_retries(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"g": {"threshold": 5}})
        original_write = plugin._write_temp_state
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)), mock.patch.object(plugin, "_write_temp_state", side_effect=OSError(errno.EIO, "temp write failed")):
            plugin._load_state()
        self.assertTrue(plugin._migration_pending)
        self.assertEqual(self.state(plugin).threshold, 5)
        with mock.patch.object(plugin, "_write_temp_state", wraps=original_write):
            plugin._save_state()
        self.assertTrue(plugin._state_file.exists())
        self.assertFalse(plugin._migration_pending)
        self.assertEqual(list(self.official.glob("*.tmp")), [])

    def test_target_appeared_race_adopts_valid_winner(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"loser": {"threshold": 4}})
        original_link = os.link
        def race(temp, target):
            self.write_state(Path(target), {"winner": {"threshold": 13}})
            raise FileExistsError(errno.EEXIST, "raced")
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)), mock.patch.object(main.os, "link", side_effect=race):
            plugin._load_state()
        self.assertEqual(set(plugin._groups), {"winner"})
        self.assertEqual(self.state(plugin, "winner").threshold, 13)
        self.assertFalse(plugin._migration_pending)
        self.assertEqual(list(self.official.glob("*.tmp")), [])
        self.assertIsNotNone(original_link)

    def test_corrupt_race_winner_blocks_overwrite(self):
        plugin = self.make_plugin(load=False)
        source = self.legacy_paths(plugin)[0]
        self.write_state(source, {"legacy": {"threshold": 4}})
        def race(temp, target):
            Path(target).write_bytes(b"not-json")
            raise FileExistsError(errno.EEXIST, "raced")
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)), mock.patch.object(main.os, "link", side_effect=race):
            plugin._load_state()
        self.assertTrue(plugin._state_load_blocked)
        self.assertEqual(plugin._state_file.read_bytes(), b"not-json")
        plugin._save_state()
        self.assertEqual(plugin._state_file.read_bytes(), b"not-json")
        self.assertEqual(list(self.official.glob("*.tmp")), [])

    def test_normal_replace_failure_preserves_old_target_and_cleans_temp(self):
        plugin = self.make_plugin(load=False)
        self.write_state(plugin._state_file, {"old": {"threshold": 6}})
        original = plugin._state_file.read_bytes()
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            plugin._load_state()
        self.state(plugin, "old").threshold = 14
        with mock.patch.object(main.os, "replace", side_effect=OSError(errno.EIO, "replace failed")):
            plugin._save_state()
        self.assertEqual(plugin._state_file.read_bytes(), original)
        self.assertEqual(list(self.official.glob("*.tmp")), [])

    def test_migration_publication_is_idempotent_on_second_startup(self):
        plugin = self.make_plugin(load=False)
        source = plugin._legacy_state_files()[0]
        self.write_state(source, {"g": {"enabled": False, "threshold": 17, "cooldown": 2.5}})
        original = source.read_bytes()
        with mock.patch.object(main, "__file__", str(self.module_path)), mock.patch("os.getcwd", return_value=str(self.cwd)):
            plugin._load_state()
            self.assertTrue(plugin._state_file.exists(), f"migration did not publish: {LOGGER.warnings}")
            second = self.make_plugin()
        self.assertIn("g", second._groups, f"second load failed: {second._state_file} exists={second._state_file.exists()} warnings={LOGGER.warnings}")
        self.assertEqual(self.state(second).threshold, 17)
        self.assertFalse(self.state(second).enabled)
        self.assertEqual(source.read_bytes(), original)
        self.assertFalse(second._migration_pending)


if __name__ == "__main__":
    unittest.main()
