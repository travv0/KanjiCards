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
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
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
        "auto_suspend_vocab": False,
        "auto_suspend_tag": "",
    }
    base.update(overrides)
    return kanjicards_module.AddonConfig(**base)


def test_chunk_sequence_splits_and_validates(kanjicards_module):
    chunks = list(kanjicards_module._chunk_sequence([1, 2, 3, 4, 5], 2))
    assert chunks == [[1, 2], [3, 4], [5]]
    with pytest.raises(ValueError):
        list(kanjicards_module._chunk_sequence([1], 0))


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
