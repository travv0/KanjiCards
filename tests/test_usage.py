import types

import pytest


class FakeDB:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def all(self, sql, *params):
        self.calls.append((sql, params))
        return list(self._rows)


class FakeCollection:
    def __init__(self, rows):
        self.db = FakeDB(rows)


@pytest.fixture
def manager(kanjicards_module):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
    return manager


def make_config(kanjicards_module):
    return kanjicards_module.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=kanjicards_module.KanjiNoteTypeConfig(name="", fields={}),
        existing_tag="",
        created_tag="",
        bucket_tags={key: "" for key in kanjicards_module.BUCKET_TAG_KEYS},
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
        auto_suspend_vocab=False,
        auto_suspend_tag="",
    )


def test_collect_vocab_usage_tracks_firsts(manager, kanjicards_module):
    rows = [
        (1, "火火\x1fmeaning", 1, None, 15),
        (2, "火曜\x1fmeaning", 0, 10, None),
        (3, "水\x1fmeaning", 0, 20, None),
    ]
    collection = FakeCollection(rows)
    model = {
        "id": 1,
        "name": "Vocab",
        "flds": [{"name": "Expression"}],
    }
    usage = manager._collect_vocab_usage(collection, [(model, [0])], make_config(kanjicards_module))
    fire_info = usage["火"]
    assert fire_info.reviewed is True
    assert fire_info.vocab_occurrences == 2
    assert fire_info.first_review_due == 15
    assert fire_info.first_new_due == 10
    assert fire_info.first_new_order == 0
    assert fire_info.first_review_order == 0

    weekday_info = usage["曜"]
    assert weekday_info.reviewed is False
    assert weekday_info.first_new_due == 10
    assert weekday_info.vocab_occurrences == 1

    water_info = usage["水"]
    assert water_info.first_new_due == 20
    assert water_info.first_new_order == 1
