import types

import pytest


class FakeNote:
    def __init__(self, note_id: int, tags=None):
        self.id = note_id
        self.tags = list(tags or [])
        self._fields = {}
        self.flush_count = 0

    def add_tag(self, tag: str) -> None:
        if tag not in self.tags:
            self.tags.append(tag)

    def remove_tag(self, tag: str) -> None:
        if tag in self.tags:
            self.tags.remove(tag)

    addTag = add_tag
    removeTag = remove_tag

    def __getitem__(self, key: str) -> str:
        if key not in self._fields:
            raise KeyError(key)
        return self._fields[key]

    def __setitem__(self, key: str, value: str) -> None:
        self._fields[key] = value

    def flush(self) -> None:
        self.flush_count += 1


class FakeCollection:
    def __init__(self, notes):
        self._notes = notes

    def get_note(self, note_id: int):
        return self._notes[note_id]


@pytest.fixture
def manager(kanjicards_module):
    manager = kanjicards_module.KanjiVocabRecalcManager.__new__(kanjicards_module.KanjiVocabRecalcManager)
    manager.mw = types.SimpleNamespace()
    manager._profile_config_error_logged = False
    manager._profile_state_error_logged = False
    manager._prioritysieve_waiting_post_sync = False
    manager._prioritysieve_toolbar_triggered = False
    manager._missing_deck_logged = False
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._last_synced_config_hash = None
    manager._pending_config_hash = None
    manager._suppress_next_auto_sync = False
    return manager


def make_config(kanjicards_module, **overrides):
    base = {
        "vocab_note_types": [],
        "kanji_note_type": kanjicards_module.KanjiNoteTypeConfig(name="", fields={}),
        "existing_tag": "",
        "created_tag": "",
        "bucket_tags": {key: "" for key in kanjicards_module.BUCKET_TAG_KEYS},
        "only_new_vocab_tag": "",
        "no_vocab_tag": "",
        "dictionary_file": "",
        "kanji_deck_name": "",
        "auto_run_on_sync": False,
        "realtime_review": False,
        "unsuspended_tag": "",
        "reorder_mode": "vocab",
        "ignore_suspended_vocab": False,
        "known_kanji_interval": 21,
        "auto_suspend_vocab": False,
        "auto_suspend_tag": "",
        "resuspend_reviewed_low_interval": False,
        "low_interval_vocab_tag": "",
        "store_scheduling_info": False,
    }
    base.update(overrides)
    return kanjicards_module.AddonConfig(**base)


def test_chunk_sequence_splits_and_validates(kanjicards_module):
    chunks = list(kanjicards_module._chunk_sequence([1, 2, 3, 4, 5], 2))
    assert chunks == [[1, 2], [3, 4], [5]]
    with pytest.raises(ValueError):
        list(kanjicards_module._chunk_sequence([1], 0))


def test_have_vocab_notes_changed_initial_run(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    monkeypatch.setattr(manager, "_resolve_vocab_models", lambda *args, **kwargs: [])
    assert manager._have_vocab_notes_changed(types.SimpleNamespace(), cfg) is True


def test_have_vocab_notes_changed_no_change(manager, kanjicards_module, monkeypatch):
    manager._last_vocab_sync_mod = 100
    manager._last_vocab_sync_count = 5
    cfg = make_config(kanjicards_module)
    monkeypatch.setattr(manager, "_resolve_vocab_models", lambda *args, **kwargs: [({}, [])])
    monkeypatch.setattr(manager, "_compute_vocab_sync_marker", lambda *args, **kwargs: (5, 100))
    assert manager._have_vocab_notes_changed(types.SimpleNamespace(), cfg) is False


def test_have_vocab_notes_changed_detects_change(manager, kanjicards_module, monkeypatch):
    manager._last_vocab_sync_mod = 100
    manager._last_vocab_sync_count = 5
    cfg = make_config(kanjicards_module)
    monkeypatch.setattr(manager, "_resolve_vocab_models", lambda *args, **kwargs: [({}, [])])
    monkeypatch.setattr(manager, "_compute_vocab_sync_marker", lambda *args, **kwargs: (6, 120))
    assert manager._have_vocab_notes_changed(types.SimpleNamespace(), cfg) is True


def test_remove_tag_case_insensitive(kanjicards_module):
    note = FakeNote(1, tags=["Tag", "other"])
    removed = kanjicards_module._remove_tag_case_insensitive(note, "tag")
    assert removed is True
    assert "Tag" not in note.tags


def test_ensure_note_tagged(manager, kanjicards_module):
    note = FakeNote(1, tags=["existing"])
    collection = FakeCollection({1: note})
    changed, fetched = manager._ensure_note_tagged(collection, 1, "new")
    assert changed is True
    assert "new" in note.tags
    assert fetched is note
    assert note.flush_count == 1
    changed_again, _ = manager._ensure_note_tagged(collection, 1, "new")
    assert changed_again is False
    assert note.flush_count == 1


def test_apply_bucket_tag_to_note(manager, kanjicards_module):
    note = FakeNote(2, tags=["untouched"])
    collection = FakeCollection({2: note})
    bucket_tag_map = {0: "bucketA", 1: "bucketB"}
    active = {"bucketA", "bucketB"}
    changed = manager._apply_bucket_tag_to_note(collection, 2, 0, bucket_tag_map, active)
    assert changed is True
    assert "bucketA" in note.tags
    assert note.flush_count == 1

    changed_second = manager._apply_bucket_tag_to_note(collection, 2, 1, bucket_tag_map, active)
    assert changed_second is True
    assert "bucketA" not in note.tags
    assert "bucketB" in note.tags
    assert note.flush_count == 2

    cleared = manager._apply_bucket_tag_to_note(collection, 2, None, bucket_tag_map, active)
    assert cleared is True
    assert "bucketB" not in note.tags
    assert note.flush_count == 3


def test_update_kanji_status_tags_transitions(manager, kanjicards_module):
    note = FakeNote(3)
    cfg = make_config(
        kanjicards_module,
        only_new_vocab_tag="only",
        no_vocab_tag="novocab",
    )
    manager._update_kanji_status_tags(note, cfg, has_vocab=True, has_reviewed_vocab=False)
    assert "only" in note.tags
    assert note.flush_count == 1

    manager._update_kanji_status_tags(note, cfg, has_vocab=True, has_reviewed_vocab=True)
    assert "only" not in note.tags
    assert note.flush_count == 2

    manager._update_kanji_status_tags(note, cfg, has_vocab=False, has_reviewed_vocab=False)
    assert "novocab" in note.tags
    assert note.flush_count == 3


def test_format_helpers_and_update_frequency(manager):
    note = FakeNote(4)
    assert manager._format_readings(["a", "", "b"]) == "a; b"
    assert manager._format_readings("x") == "x"
    assert manager._format_frequency_value(10) == "10"
    assert manager._format_frequency_value(9.5) == "9"
    assert manager._format_frequency_value(" 7 ") == "7"

    changed = manager._update_frequency_field(note, "Frequency", 5)
    assert changed is True
    assert note["Frequency"] == "5"
    unchanged = manager._update_frequency_field(note, "Frequency", 5)
    assert unchanged is False


def test_assign_field_handles_missing_name(manager):
    note = FakeNote(5)
    manager._assign_field(note, "Field", "Value")
    assert note["Field"] == "Value"
    manager._assign_field(note, None, "Ignored")
    assert note._fields["Field"] == "Value"


def test_unsuspend_note_cards_if_needed(manager, kanjicards_module, monkeypatch):
    note = FakeNote(6, tags=[])
    collection = types.SimpleNamespace(conf={"leechTag": "Leech"})
    monkeypatch.setattr(
        kanjicards_module,
        "_db_all",
        lambda *args, **kwargs: [(11, -1), (12, -1)],
    )
    calls = []
    monkeypatch.setattr(
        kanjicards_module,
        "_unsuspend_cards",
        lambda col, ids: calls.extend(ids),
    )
    count = manager._unsuspend_note_cards_if_needed(collection, note, "Unsuspend")
    assert count == 2
    assert calls == [11, 12]
    assert "Unsuspend" in note.tags
    assert note.flush_count == 1


def test_unsuspend_note_cards_if_needed_skips_leech(manager, kanjicards_module, monkeypatch):
    note = FakeNote(7, tags=["Leech"])
    collection = types.SimpleNamespace(conf={"leechTag": "Leech"})
    called = []
    monkeypatch.setattr(kanjicards_module, "_db_all", lambda *args, **kwargs: called.append(True))
    count = manager._unsuspend_note_cards_if_needed(collection, note, "Unsuspend")
    assert count == 0
    assert called == []


def test_deck_entry_name_variants(manager):
    entry_obj = types.SimpleNamespace(name="DeckObj")
    assert manager._deck_entry_name(entry_obj) == "DeckObj"
    entry_tuple = ("DeckTuple", 2)
    assert manager._deck_entry_name(entry_tuple) == "DeckTuple"
    entry_str = "DeckStr"
    assert manager._deck_entry_name(entry_str) == "DeckStr"
    assert manager._deck_entry_name(None) is None


def test_lookup_deck_id_prefers_methods(manager):
    decks = types.SimpleNamespace(
        id_for_name=lambda name: 321 if name == "Target" else None,
        all_names_and_ids=lambda: [],
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._lookup_deck_id(collection, "Target") == 321


def test_lookup_deck_id_scans_entries(manager):
    decks = types.SimpleNamespace(
        all_names_and_ids=lambda: [types.SimpleNamespace(name="Target", id=654), ("Other", 777), ("Target", 888)],
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._lookup_deck_id(collection, "Target") == 654


def test_resolve_deck_id_uses_lookup(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module, kanji_deck_name="Target")
    collection = types.SimpleNamespace(decks=types.SimpleNamespace())
    monkeypatch.setattr(manager, "_lookup_deck_id", lambda *args: 123)
    manager._missing_deck_logged = True
    result = manager._resolve_deck_id(collection, {"did": 55}, cfg)
    assert result == 123
    assert manager._missing_deck_logged is False


def test_resolve_deck_id_falls_back_to_model(manager, kanjicards_module, capsys, monkeypatch):
    cfg = make_config(kanjicards_module, kanji_deck_name="Missing")
    decks = types.SimpleNamespace(
        get_current_id=lambda: None,
        current=lambda: None,
        id=lambda name: None,
        all_names_and_ids=lambda: [],
    )
    collection = types.SimpleNamespace(decks=decks)
    monkeypatch.setattr(manager, "_lookup_deck_id", lambda *args: None)
    capsys.readouterr()
    result = manager._resolve_deck_id(collection, {"did": 77}, cfg)
    assert result == 77
    assert manager._missing_deck_logged is True
    assert "Configured kanji deck" in capsys.readouterr().out


def test_resolve_deck_id_checks_deck_helpers(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, kanji_deck_name="")
    decks = types.SimpleNamespace(
        get_current_id=lambda: 88,
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._resolve_deck_id(collection, {"did": None}, cfg) == 88


def test_resolve_deck_id_checks_current_dict(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, kanji_deck_name="")
    decks = types.SimpleNamespace(
        get_current_id=lambda: None,
        current=lambda: {"id": 99},
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._resolve_deck_id(collection, {"did": None}, cfg) == 99


def test_resolve_deck_id_checks_named_lookup(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, kanji_deck_name="")
    decks = types.SimpleNamespace(
        get_current_id=lambda: None,
        current=lambda: None,
        id=lambda name: 111 if name == "Default" else None,
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._resolve_deck_id(collection, {"did": None}, cfg) == 111


def test_resolve_deck_id_scans_all_names(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, kanji_deck_name="")
    decks = types.SimpleNamespace(
        get_current_id=lambda: None,
        current=lambda: None,
        id=lambda name: (_ for _ in ()).throw(RuntimeError("unsupported")),
        all_names_and_ids=lambda: [types.SimpleNamespace(id=222, name="Deck")],
    )
    collection = types.SimpleNamespace(decks=decks)
    assert manager._resolve_deck_id(collection, {"did": None}, cfg) == 222


def test_resolve_deck_id_raises_when_missing(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, kanji_deck_name="")
    decks = types.SimpleNamespace(
        get_current_id=lambda: None,
        current=lambda: None,
        id=lambda name: (_ for _ in ()).throw(RuntimeError("unsupported")),
        all_names_and_ids=lambda: [],
    )
    collection = types.SimpleNamespace(decks=decks)
    with pytest.raises(RuntimeError):
        manager._resolve_deck_id(collection, {"did": None}, cfg)
