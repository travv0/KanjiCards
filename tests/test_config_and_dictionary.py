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
    raw = {"reviewed_vocab": " reviewed  ", "unreviewed_vocab": None}
    normalized = manager._normalize_bucket_tags(raw)
    assert normalized["reviewed_vocab"] == "reviewed"
    assert normalized["unreviewed_vocab"] == ""
    assert normalized["no_vocab"] == ""


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
