import json
import os
import types
from pathlib import Path

import pytest


@pytest.fixture
def manager(kanjicards_module, tmp_path):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
    manager.addon_name = "KanjiCards"
    manager.addon_dir = str(tmp_path)
    manager._dictionary_cache = None
    manager._existing_notes_cache = None
    manager._kanji_model_cache = None
    manager._vocab_model_cache = None
    manager._realtime_error_logged = False
    manager._missing_deck_logged = False
    manager._sync_hook_installed = False
    manager._sync_hook_target = None
    manager._profile_config_error_logged = False
    manager._pre_answer_card_state = {}
    manager._last_question_card_id = None
    manager._debug_enabled = False
    manager._debug_path = None
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._suppress_next_auto_sync = False
    return manager


def test_normalize_kanji_fields_populates_defaults(manager):
    raw = {"kanji": "Character", "definition": None, "stroke_count": 4, "extra": 7}
    normalized = manager._normalize_kanji_fields(raw)
    assert normalized["kanji"] == "Character"
    assert normalized["definition"] == ""
    assert normalized["stroke_count"] == "4"
    assert normalized["extra"] == "7"
    for required in ("kunyomi", "onyomi", "frequency"):
        assert required in normalized


def test_normalize_bucket_tags_trim(manager):
    raw = {"reviewed_vocab": " reviewed  ", "unreviewed_vocab": None, "no_vocab": 42}
    normalized = manager._normalize_bucket_tags(raw)
    assert normalized["reviewed_vocab"] == "reviewed"
    assert normalized["unreviewed_vocab"] == ""
    assert normalized["no_vocab"] == "42"


def test_normalize_bucket_tags_default(manager):
    normalized = manager._normalize_bucket_tags(None)
    assert normalized == {"reviewed_vocab": "", "unreviewed_vocab": "", "no_vocab": ""}


def test_config_roundtrip(manager, kanjicards_module):
    raw = {
        "vocab_note_types": [
            {"note_type": "Voc", "fields": ["Expression", 42, None]},
            {"note_type": "Empty", "fields": []},
        ],
        "kanji_note_type": {
            "name": "Kanji",
            "fields": {"kanji": "Character", "frequency": None},
        },
        "existing_tag": "has_vocab_kanji",
        "created_tag": "auto",
        "bucket_tags": {"reviewed_vocab": "rev"},
        "only_new_vocab_tag": "only_new",
        "no_vocab_tag": "no_vocab",
        "dictionary_file": "dict.json",
        "kanji_deck_name": "KanjiDeck",
        "auto_run_on_sync": True,
        "realtime_review": False,
        "unsuspended_tag": "unsuspended",
        "reorder_mode": "frequency",
        "ignore_suspended_vocab": True,
        "auto_suspend_vocab": True,
        "auto_suspend_tag": "auto_suspend",
    }
    cfg = manager._config_from_raw(raw)
    assert len(cfg.vocab_note_types) == 2
    assert cfg.vocab_note_types[0].fields == ["Expression"]
    assert cfg.kanji_note_type.fields["frequency"] == ""
    serialized = manager._serialize_config(cfg)
    assert serialized["existing_tag"] == "has_vocab_kanji"
    assert serialized["kanji_note_type"]["fields"]["kanji"] == "Character"
    assert serialized["bucket_tags"]["reviewed_vocab"] == "rev"


def test_config_from_raw_missing_field(manager, kanjicards_module):
    raw = {
        "vocab_note_types": [
            {"note_type": "Valid", "fields": ["Front", "Missing"]},
        ],
        "kanji_note_type": {"name": "Kanji", "fields": {"kanji": "Character"}},
    }
    cfg = manager._config_from_raw(raw)
    assert cfg.vocab_note_types[0].fields == ["Front", "Missing"]


def test_config_from_raw_handles_invalid_entries(manager):
    raw = {
        "vocab_note_types": ["bad", {"note_type": "Valid", "fields": ["Front"]}],
        "kanji_note_type": ["not a dict"],
    }
    cfg = manager._config_from_raw(raw)
    assert cfg.vocab_note_types[0].name == "Valid"
    assert cfg.kanji_note_type.name == ""


def test_load_dictionary_json_frequency_normalization(manager):
    path = Path(manager.addon_dir) / "dictionary.json"
    payload = {
        "火": {"definition": "fire", "frequency": "123"},
        "林": {"definition": "woods", "frequency": 200.0},
        "水": {"definition": "water", "frequency": "N/A"},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    data = manager._load_dictionary_json(str(path))
    assert data["火"]["frequency"] == 123
    assert data["林"]["frequency"] == 200
    assert data["水"]["frequency"] is None


def test_load_dictionary_json_invalid(manager):
    bad_path = Path(manager.addon_dir) / "invalid.json"
    bad_path.write_text("[]", encoding="utf-8")
    with pytest.raises(RuntimeError):
        manager._load_dictionary_json(str(bad_path))


def test_load_dictionary_uses_cache_and_relative_path(manager):
    path = Path(manager.addon_dir) / "cache.json"
    path.write_text(json.dumps({"火": {"definition": "fire"}}), encoding="utf-8")
    data_first = manager._load_dictionary("cache.json")
    assert data_first["火"]["definition"] == "fire"
    path.write_text(json.dumps({"火": {"definition": "updated"}}), encoding="utf-8")
    current_mtime = os.path.getmtime(path)
    os.utime(path, (current_mtime + 1, current_mtime + 1))
    data_second = manager._load_dictionary("cache.json")
    assert data_second["火"]["definition"] == "updated"
    assert data_first is not data_second


def test_load_dictionary_missing_file(manager):
    with pytest.raises(RuntimeError):
        manager._load_dictionary("missing.json")


def test_load_dictionary_handles_unknown_extension(manager):
    path = Path(manager.addon_dir) / "dictionary.data"
    payload = {"火": {"frequency": "21"}}
    path.write_text(json.dumps(payload), encoding="utf-8")
    data = manager._load_dictionary(str(path))
    assert data["火"]["frequency"] == 21


def test_load_dictionary_kanjidic_parses_entries(manager):
    xml_path = Path(manager.addon_dir) / "kanjidic.xml"
    xml_path.write_text(
        """
        <kanjidic2>
          <character>
            <literal>火</literal>
            <misc>
              <stroke_count>4</stroke_count>
              <freq>3</freq>
            </misc>
            <reading_meaning>
              <rmgroup>
                <reading r_type="ja_on">カ</reading>
                <reading r_type="ja_on">カ</reading>
                <reading r_type="ja_kun">ひ</reading>
                <meaning>fire</meaning>
                <meaning m_lang="fr">feu</meaning>
              </rmgroup>
            </reading_meaning>
          </character>
          <character>
            <literal>林</literal>
            <misc>
              <stroke_count>8</stroke_count>
            </misc>
            <reading_meaning>
              <rmgroup>
                <meaning>woods</meaning>
              </rmgroup>
            </reading_meaning>
          </character>
        </kanjidic2>
        """,
        encoding="utf-8",
    )
    data = manager._load_dictionary_kanjidic(str(xml_path))
    assert data["火"]["definition"] == "fire"
    assert data["火"]["kunyomi"] == ["ひ"]
    assert data["火"]["onyomi"] == ["カ"]
    assert data["火"]["frequency"] == 3
    assert data["林"]["stroke_count"] == 8


def test_load_dictionary_kanjidic_invalid_xml(manager):
    xml_path = Path(manager.addon_dir) / "bad.xml"
    xml_path.write_text("<notxml>", encoding="utf-8")
    with pytest.raises(RuntimeError):
        manager._load_dictionary_kanjidic(str(xml_path))


def test_profile_config_path(manager_with_profile, tmp_path):
    expected = Path(manager_with_profile._profile_config_path())
    assert expected.name == "kanjicards_config.json"
    assert expected.parent == tmp_path / "profile"


def test_debug_writes_payload(manager_with_profile):
    manager_with_profile._debug_enabled = True
    manager_with_profile._debug("event", value=object())
    contents = Path(manager_with_profile._debug_path).read_text(encoding="utf-8")
    assert "event" in contents


def test_load_profile_config_roundtrip(manager_with_profile):
    path = Path(manager_with_profile._profile_config_path())
    payload = {"existing_tag": "profile_tag"}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    data = manager_with_profile._load_profile_config()
    assert data == payload


def test_load_profile_config_handles_invalid_json(manager_with_profile, capsys):
    path = Path(manager_with_profile._profile_config_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{bad json", encoding="utf-8")
    data = manager_with_profile._load_profile_config()
    captured = capsys.readouterr()
    assert data == {}
    assert "[KanjiCards] Failed to load profile config" in captured.out
    # Second call should not log again.
    capsys.readouterr()
    manager_with_profile._load_profile_config()
    assert capsys.readouterr().out == ""


def test_load_profile_config_or_seed_seeds_profile(manager_with_profile):
    path = Path(manager_with_profile._profile_config_path())
    global_cfg = {"existing_tag": "seeded"}
    assert not path.exists()
    data = manager_with_profile._load_profile_config_or_seed(global_cfg)
    assert path.exists()
    assert isinstance(data, dict)


def test_write_profile_config_creates_directory(manager_with_profile, tmp_path):
    path = Path(manager_with_profile._profile_config_path())
    if path.parent.exists():
        for child in path.parent.iterdir():
            child.unlink()
        path.parent.rmdir()
    manager_with_profile._write_profile_config({"value": 3})
    assert path.exists()
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["value"] == 3


def test_merge_config_sources_nested(manager_with_profile):
    global_cfg = {"nested": {"value": 1, "other": 2}, "plain": 3}
    profile_cfg = {"nested": {"value": 99}, "plain": 4}
    merged = manager_with_profile._merge_config_sources(global_cfg, profile_cfg)
    assert merged["nested"]["value"] == 99
    assert merged["nested"]["other"] == 2
    assert merged["plain"] == 4


def test_load_config_uses_profile_and_global(manager_with_profile, kanjicards_module):
    manager_with_profile.mw.addonManager._config = {
        "existing_tag": "global_tag",
        "kanji_note_type": {"name": "Kanji", "fields": {}},
    }
    profile_path = Path(manager_with_profile._profile_config_path())
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        json.dumps({"existing_tag": "profile_tag", "kanji_note_type": {"name": "Profile", "fields": {}}}),
        encoding="utf-8",
    )
    cfg = manager_with_profile.load_config()
    assert cfg.existing_tag == "profile_tag"
    assert cfg.kanji_note_type.name == "Profile"


def test_save_config_writes_and_resets(manager_with_profile, kanjicards_module, monkeypatch):
    cfg = manager_with_profile._config_from_raw(
        {
            "existing_tag": "existing",
            "created_tag": "created",
            "kanji_note_type": {"name": "Kanji", "fields": {"kanji": "Character"}},
        }
    )
    manager_with_profile._dictionary_cache = {}
    manager_with_profile._existing_notes_cache = {}
    manager_with_profile._kanji_model_cache = {}
    manager_with_profile._vocab_model_cache = {}
    manager_with_profile._realtime_error_logged = True
    manager_with_profile._missing_deck_logged = True
    manager_with_profile._sync_hook_installed = True
    manager_with_profile._sync_hook_target = "sync_did_finish"

    written = {}
    monkeypatch.setattr(manager_with_profile, "_write_profile_config", lambda data: written.update(data))
    installed = {}
    monkeypatch.setattr(manager_with_profile, "_install_sync_hook", lambda: installed.setdefault("called", True))

    manager_with_profile.save_config(cfg)

    assert manager_with_profile.mw.addonManager.written_configs
    assert written["existing_tag"] == "existing"
    assert manager_with_profile._dictionary_cache is None
    assert manager_with_profile._existing_notes_cache is None
    assert manager_with_profile._kanji_model_cache is None
    assert manager_with_profile._vocab_model_cache is None
    assert manager_with_profile._realtime_error_logged is False
    assert manager_with_profile._missing_deck_logged is False
    assert manager_with_profile._sync_hook_installed is False
    assert manager_with_profile._sync_hook_target is None
    assert installed.get("called") is True


def test_load_dictionary_handles_xml_path(manager):
    xml_path = Path(manager.addon_dir) / "dict.xml"
    xml_path.write_text(
        """
        <kanjidic2>
          <character>
            <literal>火</literal>
          </character>
        </kanjidic2>
        """,
        encoding="utf-8",
    )
    data = manager._load_dictionary(str(xml_path))
    assert "火" in data
