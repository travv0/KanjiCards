import importlib
import sys
import types
from pathlib import Path

import pytest

anki = pytest.importorskip("anki", reason="Requires Anki installed for headless integration tests.")
aqt = pytest.importorskip("aqt", reason="Requires aqt installed for headless integration tests.")


@pytest.fixture(scope="module")
def real_env(tmp_path_factory):
    # Remove stubbed modules that earlier tests inject.
    for name in list(sys.modules):
        if name == "KanjiCards" or name.startswith("KanjiCards."):
            sys.modules.pop(name)
    for prefix in ("anki", "aqt"):
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name)

    try:
        anki = importlib.import_module("anki")
        aqt = importlib.import_module("aqt")
    except ModuleNotFoundError:
        pytest.skip("Anki runtime not available for headless integration tests.")
    from anki.collection import Collection

    tmp_dir = tmp_path_factory.mktemp("anki_headless")
    col_path = tmp_dir / "collection.anki2"
    col = Collection(str(col_path))

    model_manager = col.models
    decks = col.decks
    deck_id = decks.id("Default")

    # Kanji note type with dedicated fields.
    kanji_model = model_manager.new("KanjiCard")
    for name in ["Character", "Meaning", "Strokes", "Kunyomi", "Onyomi", "Frequency"]:
        model_manager.addField(kanji_model, model_manager.newField(name))
    template = model_manager.newTemplate("Card 1")
    template["qfmt"] = "{{Character}}"
    template["afmt"] = "{{Character}}<hr id=answer>{{Meaning}}"
    model_manager.addTemplate(kanji_model, template)
    model_manager.add(kanji_model)

    vocab_model = model_manager.byName("Basic")

    KC = importlib.import_module("KanjiCards")

    manager = KC.KanjiVocabSyncManager.__new__(KC.KanjiVocabSyncManager)
    dummy_addon_dir = tmp_dir / "addon"
    dummy_addon_dir.mkdir()

    class _DummyAddonManager:
        def addonFromModule(self, module):
            return "KanjiCards"

        def addonsFolder(self):
            return str(dummy_addon_dir)

        def setConfigAction(self, *args, **kwargs):
            return None

        def getConfig(self, *_args, **_kwargs):
            return {}

        def writeConfig(self, *_args, **_kwargs):
            return None

    manager.mw = types.SimpleNamespace(
        col=col,
        addonManager=_DummyAddonManager(),
    )
    manager.addon_dir = str(dummy_addon_dir)
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
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._suppress_next_auto_sync = False

    aqt.mw = manager.mw

    yield KC, manager, col, deck_id, kanji_model, vocab_model, Path(dummy_addon_dir)

    col.close()


def test_headless_apply_updates_creates_real_notes(real_env):
    KC, manager, col, deck_id, kanji_model, vocab_model, addon_path = real_env

    raw_cfg = {
        "vocab_note_types": [
            {"note_type": vocab_model["name"], "fields": ["Front", "Back"]},
            {"note_type": "Other", "fields": [123]},
        ],
        "kanji_note_type": {
            "name": kanji_model["name"],
            "fields": {"kanji": "Character", "frequency": None},
        },
        "existing_tag": "existing",
        "created_tag": "created",
        "bucket_tags": {"reviewed_vocab": "rev"},
    }
    parsed_cfg = manager._config_from_raw(raw_cfg)
    roundtrip = manager._serialize_config(parsed_cfg)
    assert roundtrip["kanji_note_type"]["fields"]["kanji"] == "Character"

    kanji_fields = {
        "kanji": "Character",
        "definition": "Meaning",
        "stroke_count": "Strokes",
        "kunyomi": "Kunyomi",
        "onyomi": "Onyomi",
        "frequency": "Frequency",
    }
    cfg = KC.AddonConfig(
        vocab_note_types=[
            KC.VocabNoteTypeConfig(name=vocab_model["name"], fields=["Front"]),
        ],
        kanji_note_type=KC.KanjiNoteTypeConfig(name=kanji_model["name"], fields=kanji_fields),
        existing_tag="existing_kanji",
        created_tag="created_kanji",
        bucket_tags={key: "" for key in KC.BUCKET_TAG_KEYS},
        only_new_vocab_tag="only_new",
        no_vocab_tag="no_vocab",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="unsuspended",
        reorder_mode="vocab",
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        known_interval_mode=KC.DEFAULT_INTERVAL_MODE,
        auto_suspend_vocab=False,
        auto_suspend_tag="needs_suspend",
    )

    kanji_model_resolved, field_indexes, kanji_field_index = manager._get_kanji_model_context(col, cfg)
    manager._normalize_bucket_tags(None)
    manager._normalize_bucket_tags({"reviewed_vocab": " keep ", "extra": "x"})
    manager._config_from_raw({"vocab_note_types": ["bad"], "kanji_note_type": []})

    bad_cfg = KC.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=KC.KanjiNoteTypeConfig(name="", fields=kanji_fields),
        existing_tag="",
        created_tag="",
        bucket_tags={key: "" for key in KC.BUCKET_TAG_KEYS},
        only_new_vocab_tag="",
        no_vocab_tag="",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="",
        reorder_mode="vocab",
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        known_interval_mode=KC.DEFAULT_INTERVAL_MODE,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
    )
    with pytest.raises(RuntimeError):
        manager._get_kanji_model_context(col, bad_cfg)
    missing_cfg = KC.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=KC.KanjiNoteTypeConfig(name="Missing", fields=kanji_fields),
        existing_tag="",
        created_tag="",
        bucket_tags={key: "" for key in KC.BUCKET_TAG_KEYS},
        only_new_vocab_tag="",
        no_vocab_tag="",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="",
        reorder_mode="vocab",
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        known_interval_mode=KC.DEFAULT_INTERVAL_MODE,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
    )
    with pytest.raises(RuntimeError):
        manager._get_kanji_model_context(col, missing_cfg)

    bad_field_cfg = KC.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=KC.KanjiNoteTypeConfig(name=kanji_model["name"], fields={**kanji_fields, "kanji": ""}),
        existing_tag="",
        created_tag="",
        bucket_tags={key: "" for key in KC.BUCKET_TAG_KEYS},
        only_new_vocab_tag="",
        no_vocab_tag="",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="",
        reorder_mode="vocab",
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        known_interval_mode=KC.DEFAULT_INTERVAL_MODE,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
    )
    with pytest.raises(RuntimeError):
        manager._get_kanji_model_context(col, bad_field_cfg)

    missing_field_cfg = KC.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=KC.KanjiNoteTypeConfig(name=kanji_model["name"], fields={**kanji_fields, "kanji": "MissingField"}),
        existing_tag="",
        created_tag="",
        bucket_tags={key: "" for key in KC.BUCKET_TAG_KEYS},
        only_new_vocab_tag="",
        no_vocab_tag="",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="",
        reorder_mode="vocab",
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        known_interval_mode=KC.DEFAULT_INTERVAL_MODE,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
    )
    with pytest.raises(RuntimeError):
        manager._get_kanji_model_context(col, missing_field_cfg)

    vocab_note = col.new_note(vocab_model)
    vocab_note["Front"] = "火山"
    vocab_note["Back"] = "volcano"
    col.add_note(vocab_note, deck_id)

    dictionary = {
        "火": {
            "definition": "fire",
            "stroke_count": 4,
            "kunyomi": ["ひ"],
            "onyomi": ["カ"],
            "frequency": 10,
        }
    }
    usage_info = {"火": KC.KanjiUsageInfo(reviewed=False, vocab_occurrences=1)}

    stats = manager._apply_kanji_updates(
        col,
        ["火"],
        dictionary,
        kanji_model_resolved,
        field_indexes,
        kanji_field_index,
        cfg,
        usage_info,
    )
    with pytest.raises(RuntimeError):
        manager._resolve_field_indexes(kanji_model_resolved, {"missing": "Nope"})

    assert stats["created"] == 1
    note_rows = col.db.all("select flds, tags from notes where mid = ?", kanji_model_resolved["id"])
    assert len(note_rows) == 1
    fields, tags = note_rows[0]
    field_values = fields.split("\x1f")
    assert field_values[field_indexes["kanji"]] == "火"
    assert "created_kanji" in tags.split()

    manager._progress_step(None, "skip")
    tracker = {"progress": types.SimpleNamespace(update="not callable")}
    manager._progress_step(tracker, "no-op")
    class FailingProgress:
        def update(self, **kwargs):
            raise TypeError("fail")

    manager._progress_step({"progress": FailingProgress(), "current": 0, "max": 1}, "TypeError")
    tracker_callable = {
        "progress": types.SimpleNamespace(update=lambda **kwargs: None),
        "current": 0,
        "max": 2,
    }
    def bad_run(fn):
        raise RuntimeError("fail")

    manager.mw.taskman = types.SimpleNamespace(run_on_main=bad_run)
    manager._progress_step(tracker_callable, "Step")

    extra_note = col.new_note(kanji_model_resolved)
    extra_note["Character"] = "水"
    col.add_note(extra_note, deck_id)
    mapping = manager._index_existing_kanji_notes(col, kanji_model_resolved, field_indexes["kanji"])
    assert isinstance(mapping, dict)
    manager._existing_notes_cache = None
    cached = manager._get_existing_kanji_notes(col, kanji_model_resolved, field_indexes["kanji"])
    assert set(mapping.keys()).issubset(set(cached.keys()))
    cached_again = manager._get_existing_kanji_notes(col, kanji_model_resolved, field_indexes["kanji"])
    assert cached_again == cached

    vocab_map = manager._get_vocab_model_map(col, cfg)
    assert vocab_model["id"] in vocab_map
