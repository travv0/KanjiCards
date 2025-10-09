import types

import pytest


class FakeNote:
    def __init__(self, note_id, model, initial=None, tags=None):
        self.id = note_id
        self.model = model
        self.mid = model["id"]
        self.tags = list(tags or [])
        self.flush_count = 0
        self._fields = {field["name"]: "" for field in model["flds"]}
        if initial:
            for key, value in initial.items():
                self._fields[key] = value

    def __getitem__(self, key):
        return self._fields.get(key, "")

    def __setitem__(self, key, value):
        self._fields[key] = value

    @property
    def fields(self):
        return [self._fields[field["name"]] for field in self.model["flds"]]

    def serialize_fields(self):
        return "\x1f".join(self.fields)

    def add_tag(self, tag):
        if tag not in self.tags:
            self.tags.append(tag)

    addTag = add_tag

    def remove_tag(self, tag):
        if tag in self.tags:
            self.tags.remove(tag)

    removeTag = remove_tag

    def flush(self):
        self.flush_count += 1


class FakeDB:
    def __init__(self, collection):
        self.collection = collection

    def all(self, sql, *params):
        sql_simple = " ".join(sql.split())
        if sql_simple.startswith("SELECT id, flds FROM notes WHERE mid = ?"):
            target_mid = params[0]
            return [
                (note.id, note.serialize_fields())
                for note in self.collection.notes.values()
                if note.mid == target_mid
            ]
        if sql_simple.startswith("SELECT id, queue FROM cards WHERE nid = ?"):
            note_id = params[0]
            return [
                (card_id, card["queue"])
                for card_id, card in self.collection.cards.items()
                if card["nid"] == note_id
            ]
        if sql_simple.startswith("SELECT COUNT(*), MAX(mod) FROM notes WHERE mid IN"):
            mids = {int(mid) for mid in params}
            matching = [note for note in self.collection.notes.values() if note.mid in mids]
            count = len(matching)
            max_mod = 0
            for note in matching:
                try:
                    note_mod = int(getattr(note, "mod", 0))
                except Exception:
                    note_mod = 0
                if note_mod > max_mod:
                    max_mod = note_mod
            return [(count, max_mod)]
        raise AssertionError(f"Unhandled SQL query in FakeDB: {sql}")

    def execute(self, sql, *params):
        sql_simple = " ".join(sql.split())
        if "SET mod = ?, usn = ?, queue = type" in sql_simple:
            mod, usn, *card_ids = params
            for card_id in card_ids:
                card = self.collection.cards[int(card_id)]
                card["mod"] = mod
                card["usn"] = usn
                card["queue"] = card.get("type", 0)
            return None
        if "SET mod = ?, usn = ?, queue = -1" in sql_simple:
            mod, usn, *card_ids = params
            for card_id in card_ids:
                card = self.collection.cards[int(card_id)]
                card["mod"] = mod
                card["usn"] = usn
                card["queue"] = -1
            return None
        raise AssertionError(f"Unhandled SQL execute in FakeDB: {sql}")


class FakeCollection:
    def __init__(self, model, notes=None, cards=None):
        self.model = model
        self.notes = {note.id: note for note in notes or []}
        self.cards = {card["id"]: dict(card) for card in cards or []}
        self._next_note_id = max(self.notes.keys(), default=0) + 1
        self.db = FakeDB(self)
        self.decks = types.SimpleNamespace(
            get_current_id=lambda: 1,
            current=lambda: {"id": 1},
        )

    def new_note(self, model):
        return FakeNote(0, model)

    def add_note(self, note, deck_id=None):
        if not getattr(note, "id", 0):
            note.id = self._next_note_id
            self._next_note_id += 1
        note.model = self.model
        note.mid = self.model["id"]
        self.notes[note.id] = note
        return True

    def get_note(self, note_id):
        return self.notes[note_id]

    def usn(self):
        return 0


@pytest.fixture
def manager(kanjicards_module):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
    manager._debug_enabled = False
    manager._debug_path = None
    manager._pre_answer_card_state = {}
    manager._last_question_card_id = None
    manager._kanji_model_cache = None
    manager._existing_notes_cache = None
    manager._dictionary_cache = None
    manager._vocab_model_cache = None
    manager._realtime_error_logged = False
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._suppress_next_auto_sync = False
    return manager


def make_config(kanjicards_module, **overrides):
    base = {
        "vocab_note_types": [],
        "kanji_note_type": kanjicards_module.KanjiNoteTypeConfig(
            name="Kanji",
            fields={
                "kanji": "Character",
                "definition": "Meaning",
                "stroke_count": "Strokes",
                "kunyomi": "Kunyomi",
                "onyomi": "Onyomi",
                "frequency": "Frequency",
            },
        ),
        "existing_tag": "existing",
        "created_tag": "created",
        "bucket_tags": {
            key: "" for key in kanjicards_module.BUCKET_TAG_KEYS
        },
        "only_new_vocab_tag": "only_new",
        "no_vocab_tag": "no_vocab",
        "dictionary_file": "kanjidic2.xml",
        "kanji_deck_name": "",
        "auto_run_on_sync": False,
        "realtime_review": False,
        "unsuspended_tag": "unsuspend",
        "reorder_mode": "vocab",
        "ignore_suspended_vocab": False,
        "known_kanji_interval": 21,
        "auto_suspend_vocab": False,
        "auto_suspend_tag": "",
    }
    base.update(overrides)
    return kanjicards_module.AddonConfig(**base)


def make_model():
    return {
        "id": 101,
        "name": "Kanji",
        "flds": [
            {"name": "Character"},
            {"name": "Meaning"},
            {"name": "Strokes"},
            {"name": "Kunyomi"},
            {"name": "Onyomi"},
            {"name": "Frequency"},
        ],
    }


def test_apply_updates_existing_note_unsuspends_and_tags(manager, kanjicards_module, monkeypatch):
    model = make_model()
    existing_note = FakeNote(
        1,
        model,
        initial={"Character": "火", "Frequency": ""},
        tags=[],
    )
    collection = FakeCollection(
        model,
        notes=[existing_note],
        cards=[
            {"id": 11, "nid": 1, "queue": -1, "type": 2, "mod": 0, "usn": 0},
        ],
    )
    cfg = make_config(kanjicards_module)
    monkeypatch.setattr(manager, "_resolve_deck_id", lambda *_: 1)

    usage = {"火": kanjicards_module.KanjiUsageInfo(reviewed=True)}
    dictionary = {"火": {"frequency": 10}}
    field_indexes = {"kanji": 0, "frequency": 5, "definition": 1, "stroke_count": 2, "kunyomi": 3, "onyomi": 4}

    stats = manager._apply_kanji_updates(
        collection,
        ["火"],
        dictionary,
        model,
        field_indexes,
        0,
        cfg,
        usage,
        existing_notes={"火": 1},
        prune_existing=False,
    )

    assert stats["existing_tagged"] == 1
    assert stats["unsuspended"] == 1
    assert existing_note.flush_count >= 1
    assert "existing" in existing_note.tags
    assert "unsuspend" in existing_note.tags
    assert collection.cards[11]["queue"] == collection.cards[11]["type"]
    assert existing_note["Frequency"] == "10"


def test_apply_updates_creates_new_notes_and_prunes_old(manager, kanjicards_module, monkeypatch):
    model = make_model()
    old_note = FakeNote(
        1,
        model,
        initial={"Character": "火", "Frequency": "5"},
        tags=["existing", "unsuspend", "created"],
    )
    collection = FakeCollection(
        model,
        notes=[old_note],
        cards=[
            {"id": 21, "nid": 1, "queue": 0, "type": 0, "mod": 0, "usn": 0},
        ],
    )
    cfg = make_config(kanjicards_module)
    monkeypatch.setattr(manager, "_resolve_deck_id", lambda *_: 1)

    usage = {"水": kanjicards_module.KanjiUsageInfo(reviewed=False)}
    dictionary = {
        "水": {
            "definition": "water",
            "stroke_count": 4,
            "kunyomi": ["みず"],
            "onyomi": ["スイ"],
            "frequency": 50,
        },
        "火": {"frequency": 12},
    }
    field_indexes = {"kanji": 0, "frequency": 5, "definition": 1, "stroke_count": 2, "kunyomi": 3, "onyomi": 4}

    stats = manager._apply_kanji_updates(
        collection,
        ["水", "木"],
        dictionary,
        model,
        field_indexes,
        0,
        cfg,
        usage,
        existing_notes={"火": 1},
        prune_existing=True,
    )

    assert stats["created"] == 1
    assert stats["tag_removed"] == 1
    assert stats["resuspended"] == 1
    assert stats["missing_dictionary"] == {"木"}

    new_notes = [note for note in collection.notes.values() if note["Character"] == "水"]
    assert len(new_notes) == 1
    new_note = new_notes[0]
    assert {"existing", "created"} <= set(new_note.tags)
    assert "only_new" in new_note.tags

    assert "existing" not in old_note.tags
    assert "unsuspend" not in old_note.tags
    assert collection.cards[21]["queue"] == -1


class SimpleNote:
    def __init__(self, note_id, tags=None):
        self.id = note_id
        self.tags = list(tags or [])
        self.flush_count = 0

    def add_tag(self, tag):
        if tag not in self.tags:
            self.tags.append(tag)

    addTag = add_tag

    def remove_tag(self, tag):
        if tag in self.tags:
            self.tags.remove(tag)

    removeTag = remove_tag

    def flush(self):
        self.flush_count += 1


def test_update_vocab_suspension_auto_suspend(manager, kanjicards_module, monkeypatch):
    cfg = make_config(
        kanjicards_module,
        auto_suspend_vocab=True,
        auto_suspend_tag="NeedsSuspend",
    )
    note = SimpleNote(101)
    collection = types.SimpleNamespace()

    monkeypatch.setattr(
        manager,
        "_collect_vocab_note_chars",
        lambda *args, **kwargs: {101: ({"火"}, set())},
    )
    monkeypatch.setattr(
        manager,
        "_compute_kanji_reviewed_flags",
        lambda *args, **kwargs: {"火": False},
    )
    monkeypatch.setattr(
        manager,
        "_load_card_status_for_notes",
        lambda *args, **kwargs: {101: [(501, 0)]},
    )
    monkeypatch.setattr(
        kanjicards_module,
        "_get_note",
        lambda *args, **kwargs: note,
    )
    monkeypatch.setattr(
        kanjicards_module,
        "_resuspend_note_cards",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        kanjicards_module,
        "_unsuspend_cards",
        lambda *args, **kwargs: None,
    )

    stats = manager._update_vocab_suspension(
        collection,
        cfg,
        {1: [0]},
        existing_notes={"火": 1},
    )

    assert stats == {"vocab_suspended": 1, "vocab_unsuspended": 0}
    assert "needssuspend" in {tag.lower() for tag in note.tags}
    assert note.flush_count == 1


def test_update_vocab_suspension_unsuspends_and_clears_tag(manager, kanjicards_module, monkeypatch):
    cfg = make_config(
        kanjicards_module,
        auto_suspend_vocab=True,
        auto_suspend_tag="NeedsSuspend",
    )
    note = SimpleNote(202, tags=["NeedsSuspend"])
    collection = types.SimpleNamespace()

    monkeypatch.setattr(
        manager,
        "_collect_vocab_note_chars",
        lambda *args, **kwargs: {202: ({"火"}, {"NeedsSuspend"})},
    )
    monkeypatch.setattr(
        manager,
        "_compute_kanji_reviewed_flags",
        lambda *args, **kwargs: {"火": True},
    )
    monkeypatch.setattr(
        manager,
        "_load_card_status_for_notes",
        lambda *args, **kwargs: {202: [(601, -1)]},
    )
    monkeypatch.setattr(
        kanjicards_module,
        "_get_note",
        lambda *args, **kwargs: note,
    )
    unsuspended = []

    def fake_unsuspend(collection, card_ids):
        unsuspended.extend(card_ids)

    monkeypatch.setattr(
        kanjicards_module,
        "_unsuspend_cards",
        fake_unsuspend,
    )
    monkeypatch.setattr(
        kanjicards_module,
        "_resuspend_note_cards",
        lambda *args, **kwargs: 0,
    )

    stats = manager._update_vocab_suspension(
        collection,
        cfg,
        {1: [0]},
        existing_notes={"火": 1},
    )

    assert stats == {"vocab_suspended": 0, "vocab_unsuspended": 1}
    assert unsuspended == [601]
    assert "needssuspend" not in {tag.lower() for tag in note.tags}
    assert note.flush_count == 1


def test_compute_kanji_reviewed_flags(manager, kanjicards_module, monkeypatch):
    calls = []

    def fake_db_all(collection, sql, *params, context=""):
        calls.append(context)
        return [(1, 1, 25), (2, 1, 10), (3, 0, 0)]

    monkeypatch.setattr(kanjicards_module, "_db_all", fake_db_all)

    existing = {"火": 1, "水": 2, "風": 3}
    result = manager._compute_kanji_reviewed_flags(types.SimpleNamespace(), existing, 21)

    assert result == {"火": True, "水": False, "風": False}
    assert calls and calls[0].startswith("compute_kanji_reviewed_flags")


def test_compute_kanji_reviewed_flags_zero_threshold(manager, kanjicards_module, monkeypatch):
    monkeypatch.setattr(
        kanjicards_module,
        "_db_all",
        lambda *args, **kwargs: [(1, 1, 5)],
    )

    result = manager._compute_kanji_reviewed_flags(types.SimpleNamespace(), {"火": 1}, 0)

    assert result == {"火": True}


def test_collect_vocab_note_chars_filters(manager, monkeypatch):
    rows = [
        (10, "火\x1freading", "tag1"),
        (11, "山\x1fmeaning", "tagA tagB"),
    ]
    monkeypatch.setattr(
        manager,
        "_fetch_vocab_rows",
        lambda *args, **kwargs: rows,
    )
    result_all = manager._collect_vocab_note_chars(types.SimpleNamespace(), {50: [0]})
    assert result_all[10][0] == {"火"}
    assert result_all[10][1] == {"tag1"}

    result_filtered = manager._collect_vocab_note_chars(
        types.SimpleNamespace(),
        {50: [0]},
        target_chars={"山"},
    )
    assert 11 in result_filtered and 10 not in result_filtered


def test_load_card_status_for_notes(manager, kanjicards_module, monkeypatch):
    def fake_db_all(collection, sql, *params, context=""):
        assert "load_card_status_for_notes" in context
        return [
            (101, 1, -1),
            (102, 2, 0),
        ]

    monkeypatch.setattr(kanjicards_module, "_db_all", fake_db_all)
    mapping = manager._load_card_status_for_notes(types.SimpleNamespace(), [1, 2])
    assert mapping == {1: [(101, -1)], 2: [(102, 0)]}


def test_load_note_active_status(manager, kanjicards_module, monkeypatch):
    def fake_db_all(collection, sql, *params, context=""):
        assert "load_note_active_status" in context
        return [
            (1, 1),
            (2, 0),
        ]

    monkeypatch.setattr(kanjicards_module, "_db_all", fake_db_all)
    status = manager._load_note_active_status(types.SimpleNamespace(), [1, 2])
    assert status == {1: True, 2: False}


def test_sync_internal_aggregates_stats(manager, kanjicards_module, monkeypatch):
    cfg = make_config(
        kanjicards_module,
        reorder_mode="frequency",
        auto_suspend_vocab=True,
        auto_suspend_tag="NeedsSuspend",
    )
    monkeypatch.setattr(manager, "load_config", lambda: cfg)
    collection = types.SimpleNamespace()
    manager.mw = types.SimpleNamespace(col=collection)
    monkeypatch.setattr(manager, "_progress_step", lambda tracker, label: None)

    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10}, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(
        manager,
        "_resolve_vocab_models",
        lambda *args, **kwargs: [({"id": 20}, [0])],
    )
    dictionary = {"火": {"frequency": 10}}
    monkeypatch.setattr(manager, "_load_dictionary", lambda *args, **kwargs: dictionary)
    usage = {"火": kanjicards_module.KanjiUsageInfo(reviewed=True)}
    monkeypatch.setattr(manager, "_collect_vocab_usage", lambda *args, **kwargs: usage)
    existing_notes = {"火": 100}
    monkeypatch.setattr(manager, "_get_existing_kanji_notes", lambda *args, **kwargs: existing_notes)
    base_stats = {
        "kanji_scanned": 1,
        "existing_tagged": 1,
        "created": 0,
        "unsuspended": 0,
        "missing_dictionary": set(),
        "tag_removed": 0,
        "resuspended": 0,
        "vocab_suspended": 0,
        "vocab_unsuspended": 0,
    }
    monkeypatch.setattr(
        manager,
        "_apply_kanji_updates",
        lambda *args, **kwargs: base_stats.copy(),
    )
    monkeypatch.setattr(
        manager,
        "_reorder_new_kanji_cards",
        lambda *args, **kwargs: {"cards_reordered": 2},
    )
    monkeypatch.setattr(
        manager,
        "_update_vocab_suspension",
        lambda *args, **kwargs: {"vocab_suspended": 3, "vocab_unsuspended": 4},
    )
    monkeypatch.setattr(
        manager,
        "_compute_vocab_sync_marker",
        lambda *args, **kwargs: (5, 123),
    )

    stats = manager._sync_internal()

    assert stats["kanji_scanned"] == 1
    assert stats["cards_reordered"] == 2
    assert stats["vocab_suspended"] == 3
    assert stats["vocab_unsuspended"] == 4


def test_create_kanji_note_populates_fields(manager, kanjicards_module, monkeypatch):
    model = make_model()
    collection = FakeCollection(model)
    cfg = make_config(kanjicards_module)
    field_indexes = {
        "kanji": 0,
        "definition": 1,
        "stroke_count": 2,
        "kunyomi": 3,
        "onyomi": 4,
        "frequency": 5,
    }
    entry = {
        "definition": "fire",
        "stroke_count": 4,
        "kunyomi": ["ひ"],
        "onyomi": ["カ"],
        "frequency": 12,
    }
    monkeypatch.setattr(manager, "_resolve_deck_id", lambda *args: 1)
    monkeypatch.setattr(kanjicards_module, "_add_note", lambda col, note, deck_id: col.add_note(note, deck_id))

    note_id = manager._create_kanji_note(
        collection,
        model,
        field_indexes,
        entry,
        "火",
        "existing",
        "created",
        cfg,
    )
    assert note_id is not None
    note = collection.get_note(note_id)
    assert note["Character"] == "火"
    assert note["Meaning"] == "fire"
    assert "existing" in note.tags and "created" in note.tags
    assert note["Kunyomi"] == "ひ"
    assert note["Onyomi"] == "カ"
    assert note["Frequency"] == "12"


def test_create_kanji_note_returns_none_when_add_fails(manager, kanjicards_module, monkeypatch):
    model = make_model()
    collection = FakeCollection(model)
    cfg = make_config(kanjicards_module)
    field_indexes = {"kanji": 0}
    entry = {}
    monkeypatch.setattr(manager, "_resolve_deck_id", lambda *args: 1)

    def fail_add_note(col, note, deck_id):
        return False

    monkeypatch.setattr(kanjicards_module, "_add_note", fail_add_note)
    result = manager._create_kanji_note(
        collection,
        model,
        field_indexes,
        entry,
        "火",
        "",
        "",
        cfg,
    )
    assert result is None


def test_sync_internal_requires_configured_kanji(manager, kanjicards_module):
    cfg = make_config(kanjicards_module)
    cfg.kanji_note_type.name = ""
    manager.load_config = lambda: cfg
    manager.mw = types.SimpleNamespace(col=types.SimpleNamespace())
    with pytest.raises(RuntimeError):
        manager._sync_internal()
