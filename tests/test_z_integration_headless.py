import importlib
import sys
import types
from pathlib import Path

import pytest


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

    anki = importlib.import_module("anki")
    aqt = importlib.import_module("aqt")
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

    aqt.mw = manager.mw

    yield KC, manager, col, deck_id, kanji_model, vocab_model, Path(dummy_addon_dir)

    col.close()


def test_headless_apply_updates_creates_real_notes(real_env):
    KC, manager, col, deck_id, kanji_model, vocab_model, addon_path = real_env

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
        auto_suspend_vocab=False,
        auto_suspend_tag="needs_suspend",
    )

    kanji_model_resolved, field_indexes, kanji_field_index = manager._get_kanji_model_context(col, cfg)

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

    assert stats["created"] == 1
    note_rows = col.db.all("select flds, tags from notes where mid = ?", kanji_model_resolved["id"])
    assert len(note_rows) == 1
    fields, tags = note_rows[0]
    field_values = fields.split("\x1f")
    assert field_values[field_indexes["kanji"]] == "火"
    assert "created_kanji" in tags.split()
