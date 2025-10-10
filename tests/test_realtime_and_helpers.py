import types

import pytest


class FakeNote:
    def __init__(self, mid, fields, note_id=1):
        self.mid = mid
        self.fields = list(fields)
        self.id = note_id


class FakeCard:
    def __init__(self, card_id, note, queue=0, type_=0, nid=None):
        self.id = card_id
        self._note = note
        self.queue = queue
        self.type = type_
        self.nid = nid if nid is not None else note.id

    def note(self):
        return self._note


class FakeModels:
    def __init__(self, models):
        self._models = {model["name"]: model for model in models}

    def byName(self, name):
        return self._models.get(name)

    def all(self):
        return list(self._models.values())


class FakeCollection:
    def __init__(self, models):
        self.models = FakeModels(models)

    def get_note(self, note_id):
        raise RuntimeError("should not be called")

    def get_card(self, card_id):
        raise RuntimeError("should not be called")


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
    manager._last_synced_config_hash = None
    manager._pending_config_hash = None
    return manager


def make_config(kanjicards_module, **overrides):
    base = {
        "vocab_note_types": [
            kanjicards_module.VocabNoteTypeConfig(name="Vocab", fields=["Expression"]),
        ],
        "kanji_note_type": kanjicards_module.KanjiNoteTypeConfig(
            name="Kanji",
            fields={
                "kanji": "Character",
                "definition": "",
                "stroke_count": "",
                "kunyomi": "",
                "onyomi": "",
                "frequency": "",
                "scheduling_info": "",
            },
        ),
        "existing_tag": "existing",
        "created_tag": "created",
        "bucket_tags": {key: "" for key in kanjicards_module.BUCKET_TAG_KEYS},
        "only_new_vocab_tag": "",
        "no_vocab_tag": "",
        "dictionary_file": "kanjidic2.xml",
        "kanji_deck_name": "",
        "auto_run_on_sync": False,
        "realtime_review": True,
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


def test_process_reviewed_card_triggers_vocab_update(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg

    kanji_model = {"id": 10, "name": "Kanji", "flds": [{"name": "Character"}]}
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: (kanji_model, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(
        manager,
        "_get_vocab_model_map",
        lambda *args, **kwargs: {1: ({"id": 1}, [0], 1.0)},
    )
    monkeypatch.setattr(
        manager,
        "_get_existing_kanji_notes",
        lambda *args, **kwargs: {"火": 1},
    )
    called = []
    monkeypatch.setattr(
        manager,
        "_update_vocab_suspension",
        lambda *args, **kwargs: called.append(kwargs["target_chars"]),
    )

    note = FakeNote(mid=10, fields=["火"], note_id=5)
    card = FakeCard(111, note)
    manager._pre_answer_card_state[111] = {"type": 0, "queue": 0, "note_id": 5}
    manager._last_question_card_id = 111
    manager.mw.col = types.SimpleNamespace()

    manager._process_reviewed_card(card)

    assert called and called[0] == {"火"}
    assert manager._pre_answer_card_state == {}


def test_process_reviewed_card_triggers_for_review_queue(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg

    kanji_model = {"id": 10, "name": "Kanji", "flds": [{"name": "Character"}]}
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: (kanji_model, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(
        manager,
        "_get_vocab_model_map",
        lambda *args, **kwargs: {1: ({"id": 1}, [0], 1.0)},
    )
    monkeypatch.setattr(
        manager,
        "_get_existing_kanji_notes",
        lambda *args, **kwargs: {"火": 1},
    )
    called = []
    monkeypatch.setattr(
        manager,
        "_update_vocab_suspension",
        lambda *args, **kwargs: called.append(kwargs["target_chars"]),
    )

    note = FakeNote(mid=10, fields=["火"], note_id=5)
    card = FakeCard(777, note, queue=2, type_=2)
    manager._pre_answer_card_state[777] = {"type": 2, "queue": 2, "note_id": 5}
    manager._last_question_card_id = 777
    manager.mw.col = types.SimpleNamespace()

    manager._process_reviewed_card(card)

    assert called and called[0] == {"火"}
    assert manager._pre_answer_card_state == {}


def test_process_reviewed_card_fetches_note_on_failure(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg
    kanji_model = {"id": 10, "name": "Kanji", "flds": [{"name": "Character"}]}
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: (kanji_model, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(
        manager,
        "_get_vocab_model_map",
        lambda *args, **kwargs: {1: ({"id": 1}, [0], 1.0)},
    )
    monkeypatch.setattr(
        manager,
        "_get_existing_kanji_notes",
        lambda *args, **kwargs: {"火": 1},
    )
    monkeypatch.setattr(
        manager,
        "_update_vocab_suspension",
        lambda *args, **kwargs: None,
    )

    class RaisingCard(FakeCard):
        def note(self):
            raise RuntimeError("no note")

    fallback_note = FakeNote(mid=10, fields=["火"], note_id=6)
    manager.mw.col = types.SimpleNamespace(
        get_note=lambda note_id: fallback_note,
    )
    card = RaisingCard(222, FakeNote(10, ["火"], 6))
    manager._pre_answer_card_state[222] = {"type": 0, "queue": 0, "note_id": 6}

    manager._process_reviewed_card(card)

    assert manager._pre_answer_card_state == {}


def test_on_reviewer_did_show_question_handles_string_id(manager):
    card = types.SimpleNamespace(id="42", type=0, queue=0, nid=9)
    manager._on_reviewer_did_show_question(card)
    assert 42 in manager._pre_answer_card_state
    assert manager._last_question_card_id == 42


def test_on_reviewer_did_answer_card_logs_errors(manager, monkeypatch):
    manager.mw.col = types.SimpleNamespace()
    called = {}

    def failing_process(card):
        called["count"] = called.get("count", 0) + 1
        raise RuntimeError("boom")

    manager._process_reviewed_card = failing_process  # type: ignore[assignment]
    card = types.SimpleNamespace(id=5, queue=0, type=0)
    manager._on_reviewer_did_answer_card(card)
    assert called["count"] == 1
    assert manager._realtime_error_logged is True


def test_process_reviewed_card_respects_realtime_flag(manager, kanjicards_module):
    cfg = make_config(kanjicards_module, realtime_review=False)
    manager.load_config = lambda: cfg
    manager._pre_answer_card_state[7] = {"type": 0, "queue": 0, "note_id": 1}
    manager.mw.col = types.SimpleNamespace()
    card = FakeCard(7, FakeNote(10, ["火"], 1))
    manager._process_reviewed_card(card)


def test_process_reviewed_card_uses_last_question_fallback(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg
    manager.mw.col = types.SimpleNamespace()
    manager._pre_answer_card_state[9] = {"type": 1, "queue": 1, "note_id": 2}
    manager._last_question_card_id = 9
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10, "flds": [{"name": "Character"}]}, {"kanji": 0}, 0),
    )
    card = types.SimpleNamespace(id="not-number")
    manager._process_reviewed_card(card)
    assert 9 not in manager._pre_answer_card_state


def test_process_reviewed_card_skips_non_kanji_note(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg
    manager.mw.col = types.SimpleNamespace()
    manager._pre_answer_card_state[10] = {"type": 0, "queue": 0, "note_id": 3}
    card = FakeCard(10, FakeNote(99, ["火"], 3))
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10, "flds": [{"name": "Character"}]}, {"kanji": 0}, 0),
    )
    manager._process_reviewed_card(card)


def test_process_reviewed_card_empty_vocab_types(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module, vocab_note_types=[])
    manager.load_config = lambda: cfg
    manager.mw.col = types.SimpleNamespace()
    manager._pre_answer_card_state[11] = {"type": 0, "queue": 0, "note_id": 4}
    card = FakeCard(11, FakeNote(10, ["火"], 4))
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10, "flds": [{"name": "Character"}]}, {"kanji": 0}, 0),
    )
    manager._process_reviewed_card(card)


def test_process_reviewed_card_empty_vocab_map(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg
    manager.mw.col = types.SimpleNamespace()
    manager._pre_answer_card_state[12] = {"type": 0, "queue": 0, "note_id": 5}
    card = FakeCard(12, FakeNote(10, ["火"], 5))
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10, "flds": [{"name": "Character"}]}, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(manager, "_get_vocab_model_map", lambda *args, **kwargs: {})
    manager._process_reviewed_card(card)


def test_process_reviewed_card_no_kanji_chars(manager, kanjicards_module, monkeypatch):
    cfg = make_config(kanjicards_module)
    manager.load_config = lambda: cfg
    manager.mw.col = types.SimpleNamespace()
    manager._pre_answer_card_state[13] = {"type": 0, "queue": 0, "note_id": 6}
    card = FakeCard(13, FakeNote(10, ["abc"], 6))
    monkeypatch.setattr(
        manager,
        "_get_kanji_model_context",
        lambda *args, **kwargs: ({"id": 10, "flds": [{"name": "Character"}]}, {"kanji": 0}, 0),
    )
    monkeypatch.setattr(
        manager,
        "_get_vocab_model_map",
        lambda *args, **kwargs: {1: ({"id": 1}, [0], 1.0)},
    )
    manager._process_reviewed_card(card)


def test_stats_warrant_sync_checks_keys(manager):
    stats_true = {"created": 1, "other": "0"}
    assert manager._stats_warrant_sync(stats_true) is True
    stats_false = {"created": "0", "existing_tagged": 0}
    assert manager._stats_warrant_sync(stats_false) is False


def test_trigger_followup_sync_uses_handler(manager):
    called = {}

    def fake_sync():
        called["sync"] = True

    manager.mw = types.SimpleNamespace(onSync=fake_sync)
    assert manager._trigger_followup_sync() is True
    assert called["sync"] is True


def test_get_kanji_model_context_caches(manager, kanjicards_module):
    model = {
        "id": 77,
        "name": "Kanji",
        "flds": [{"name": "Character"}, {"name": "Meaning"}],
    }
    collection = FakeCollection([model])
    cfg = make_config(kanjicards_module)

    kanji_model, field_indexes, index = manager._get_kanji_model_context(collection, cfg)
    assert kanji_model is model
    assert field_indexes["kanji"] == 0
    assert index == 0

    # Second call should hit cache with same result
    kanji_model_cached, _, _ = manager._get_kanji_model_context(collection, cfg)
    assert kanji_model_cached is model


def test_resolve_vocab_models_filters_missing_fields(manager, kanjicards_module):
    vocab_model = {
        "id": 1,
        "name": "Vocab",
        "flds": [{"name": "Expression"}, {"name": "Reading"}],
    }
    collection = FakeCollection([vocab_model])
    cfg = make_config(kanjicards_module)
    results = manager._resolve_vocab_models(collection, cfg)
    assert results == [(vocab_model, [0], 1.0)]


def test_resolve_field_indexes_maps_names(manager):
    model = {"name": "Kanji", "flds": [{"name": "Character"}, {"name": "Meaning"}]}
    mapping = {"kanji": "Character", "definition": "Meaning"}
    indexes = manager._resolve_field_indexes(model, mapping)  # type: ignore[arg-type]
    assert indexes == {"kanji": 0, "definition": 1}


def test_get_vocab_model_map_uses_cache(manager, kanjicards_module):
    model = {
        "id": 1,
        "name": "Vocab",
        "flds": [{"name": "Expression"}],
    }
    collection = FakeCollection([model])
    cfg = make_config(kanjicards_module)

    mapping = manager._get_vocab_model_map(collection, cfg)
    assert 1 in mapping

    # Update models to ensure cache prevents extra lookup
    collection.models._models.clear()
    cached = manager._get_vocab_model_map(collection, cfg)
    assert cached == mapping


def test_notify_summary_formats_message(manager, kanjicards_module, monkeypatch):
    messages = []

    def fake_tooltip(message, **kwargs):
        messages.append(message)

    monkeypatch.setattr(kanjicards_module, "tooltip", fake_tooltip)
    stats = {
        "kanji_scanned": 3,
        "created": 1,
        "existing_tagged": 2,
        "unsuspended": 1,
        "tag_removed": 1,
        "resuspended": 1,
        "vocab_suspended": 1,
        "vocab_unsuspended": 1,
        "missing_dictionary": {"火", "水"},
    }
    manager._notify_summary(stats)
    assert messages


def test_progress_step_runs_update(manager, kanjicards_module, monkeypatch):
    updates = []

    class FakeProgress:
        def update(self, **kwargs):
            updates.append(kwargs)

    class FakeTaskman:
        def run_on_main(self, callback):
            callback()

    manager.mw = types.SimpleNamespace(taskman=FakeTaskman())
    tracker = {"progress": FakeProgress(), "current": 0, "max": 2}
    monkeypatch.setattr(
        kanjicards_module.QApplication,
        "processEvents",
        staticmethod(lambda: (_ for _ in ()).throw(RuntimeError("process failure"))),
    )
    manager._progress_step(tracker, "Step")
    assert updates and updates[0]["label"] == "Step"
    assert tracker["current"] == 1
