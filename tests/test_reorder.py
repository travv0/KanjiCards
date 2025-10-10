import types

import pytest


class FakeNote:
    def __init__(self, note_id, mid, fields, tags=None):
        self.id = note_id
        self.mid = mid
        self.field_names = ["Character", "Frequency"]
        self.fields = list(fields)
        self.tags = list(tags or [])
        self.flush_count = 0

    def serialize_fields(self):
        return "\x1f".join(self.fields)

    def tag_string(self):
        return " ".join(self.tags)

    def add_tag(self, tag):
        if tag not in self.tags:
            self.tags.append(tag)

    def remove_tag(self, tag):
        if tag in self.tags:
            self.tags.remove(tag)

    addTag = add_tag
    removeTag = remove_tag

    def __getitem__(self, key):
        for name, value in zip(self.field_names, self.fields):
            if name == key:
                return value
        raise KeyError(key)

    def __setitem__(self, key, value):
        for index, name in enumerate(self.field_names):
            if name == key:
                self.fields[index] = value
                return
        self.fields.append(value)
        self.field_names.append(key)

    def flush(self):
        self.flush_count += 1


class FakeDB:
    def __init__(self, collection):
        self.collection = collection

    def all(self, sql, *params):
        sql_simple = " ".join(sql.split())
        if "FROM cards" in sql_simple and "JOIN notes" in sql_simple and "queue = 0" in sql_simple:
            target_mid = params[0]
            rows = []
            for card in self.collection.cards.values():
                if card["queue"] != 0:
                    continue
                note = self.collection.notes[card["nid"]]
                if note.mid != target_mid:
                    continue
                rows.append(
                    (
                        card["id"],
                        card["nid"],
                        card["due"],
                        card["did"],
                        card["mod"],
                        card["usn"],
                        note.serialize_fields(),
                    )
                )
            return rows

        if sql_simple.startswith("SELECT id, tags FROM notes"):
            patterns = [param.strip("%").lower() for param in params]
            results = []
            for note in self.collection.notes.values():
                tags_str = note.tag_string()
                tag_lower = tags_str.lower()
                if any(pattern and pattern in tag_lower for pattern in patterns):
                    results.append((note.id, tags_str))
            return results
        if sql_simple.startswith("SELECT COUNT(*), MAX(mod) FROM notes WHERE mid IN"):
            mids = {int(mid) for mid in params}
            matching = [note for note in self.collection.notes.values() if note.mid in mids]
            count = len(matching)
            max_mod = 0
            return [(count, max_mod)]

        raise AssertionError(f"Unhandled SQL in test stub: {sql}")

    def execute(self, sql, *params):
        sql_simple = " ".join(sql.split())
        if sql_simple.startswith("UPDATE cards SET due = ?, mod = ?, usn = ? WHERE id = ?"):
            due, mod, usn, card_id = params
            card = self.collection.cards[card_id]
            card["due"] = due
            card["mod"] = mod
            card["usn"] = usn
            self.collection.updated_cards.append((card_id, due, mod, usn))
            return None

        raise AssertionError(f"Unhandled SQL execute in test stub: {sql}")


class FakeCollection:
    def __init__(self, notes, cards, usn=100):
        self.notes = {note.id: note for note in notes}
        self.cards = {card["id"]: dict(card) for card in cards}
        self.updated_cards = []
        self._usn = usn
        self.db = FakeDB(self)

    def usn(self):
        return self._usn

    def get_note(self, note_id):
        return self.notes[note_id]


def build_environment(kanjicards_module, reorder_mode):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._last_synced_config_hash = None
    manager._pending_config_hash = None
    manager._suppress_next_auto_sync = False

    kanji_model = {
        "id": 900,
        "name": "Kanji",
        "flds": [{"name": "Character"}, {"name": "Frequency"}],
    }
    kanji_field_index = 0

    notes = [
        FakeNote(1, 900, ["火", ""], tags=["old_tag"]),
        FakeNote(2, 900, ["土", ""], tags=[]),
        FakeNote(3, 900, ["水", ""], tags=["bucket_reviewed"]),
        FakeNote(4, 900, ["風", ""], tags=["bucket_unreviewed"]),
        FakeNote(5, 900, ["木", ""], tags=["bucket_unreviewed"]),
        FakeNote(6, 900, ["空", ""], tags=["bucket_reviewed"]),
    ]

    cards = [
        {"id": 1, "nid": 1, "due": 50, "queue": 0, "did": 1, "mod": 0, "usn": 0},
        {"id": 2, "nid": 2, "due": 40, "queue": 0, "did": 1, "mod": 0, "usn": 0},
        {"id": 3, "nid": 3, "due": 30, "queue": 0, "did": 1, "mod": 0, "usn": 0},
        {"id": 4, "nid": 4, "due": 20, "queue": 0, "did": 1, "mod": 0, "usn": 0},
        {"id": 5, "nid": 5, "due": 10, "queue": 0, "did": 1, "mod": 0, "usn": 0},
        {"id": 6, "nid": 6, "due": 0, "queue": 0, "did": 1, "mod": 0, "usn": 0},
    ]
    collection = FakeCollection(notes, cards)

    usage_info = {
        "火": kanjicards_module.KanjiUsageInfo(
            reviewed=True,
            first_review_order=0,
            first_review_due=5,
            first_new_order=3,
            first_new_due=9,
            vocab_occurrences=4,
        ),
        "土": kanjicards_module.KanjiUsageInfo(
            reviewed=True,
            first_review_order=0,
            first_review_due=5,
            first_new_order=3,
            first_new_due=9,
            vocab_occurrences=4,
        ),
        "水": kanjicards_module.KanjiUsageInfo(
            reviewed=False,
            first_new_order=0,
            first_new_due=4,
            vocab_occurrences=3,
        ),
        "風": kanjicards_module.KanjiUsageInfo(
            reviewed=False,
            first_new_order=0,
            first_new_due=4,
            vocab_occurrences=3,
        ),
    }

    dictionary = {
        "火": {"frequency": 100},
        "土": {"frequency": 100},
        "水": {"frequency": 200},
        "風": {"frequency": 200},
        "木": {"frequency": 50},
        "空": {},
    }

    cfg = kanjicards_module.AddonConfig(
        vocab_note_types=[],
        kanji_note_type=kanjicards_module.KanjiNoteTypeConfig(
            name="Kanji",
            fields={
                "kanji": "Character",
                "frequency": "Frequency",
                "definition": "",
                "stroke_count": "",
                "kunyomi": "",
                "onyomi": "",
                "scheduling_info": "",
            },
        ),
        existing_tag="existing",
        created_tag="created",
        bucket_tags={
            "reviewed_vocab": "bucket_reviewed",
            "unreviewed_vocab": "bucket_unreviewed",
            "no_vocab": "bucket_no_vocab",
        },
        only_new_vocab_tag="only_new",
        no_vocab_tag="no_vocab",
        dictionary_file="",
        kanji_deck_name="",
        auto_run_on_sync=False,
        realtime_review=False,
        unsuspended_tag="",
        reorder_mode=reorder_mode,
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
        resuspend_reviewed_low_interval=False,
        low_interval_vocab_tag="",
        store_scheduling_info=False,
    )

    initial_tags = {note.id: set(note.tags) for note in notes}

    return manager, collection, kanji_model, kanji_field_index, cfg, usage_info, dictionary, initial_tags


@pytest.mark.parametrize(
    ("mode", "expected_order"),
    [
        ("vocab", [2, 1, 4, 3, 5, 6]),
        ("vocab_frequency", [2, 1, 4, 3, 5, 6]),
        ("frequency", [5, 1, 2, 3, 4, 6]),
    ],
)
def test_reorder_new_kanji_cards_full_collection(mode, expected_order, kanjicards_module):
    (
        manager,
        collection,
        kanji_model,
        kanji_field_index,
        cfg,
        usage_info,
        dictionary,
        initial_tags,
    ) = build_environment(
        kanjicards_module, mode
    )

    stats = manager._reorder_new_kanji_cards(
        collection,
        kanji_model,
        kanji_field_index,
        cfg,
        usage_info,
        dictionary,
    )

    reordered = sorted(collection.cards.values(), key=lambda card: card["due"])
    actual_order = [card["id"] for card in reordered]
    assert actual_order == expected_order
    assert len({card["due"] for card in collection.cards.values()}) == len(collection.cards)

    assert stats["cards_reordered"] == len(collection.cards)
    assert stats["bucket_tags_updated"] > 0

    buckets = {
        0: "bucket_reviewed",
        1: "bucket_unreviewed",
        2: "bucket_no_vocab",
    }
    expected_bucket_members = {
        "bucket_reviewed": {1, 2},
        "bucket_unreviewed": {3, 4},
        "bucket_no_vocab": {5, 6},
    }
    for tag, note_ids in expected_bucket_members.items():
        for note_id in note_ids:
            assert tag in collection.notes[note_id].tags

    for note_id, note in collection.notes.items():
        original = initial_tags[note_id]
        current = set(note.tags)
        if current != original:
            assert note.flush_count >= 1

    for note_id, note in collection.notes.items():
        for tag in buckets.values():
            if note_id not in expected_bucket_members[tag]:
                assert tag not in note.tags


def test_build_reorder_key_vocab_bucket_sorting(kanjicards_module):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager._last_synced_config_hash = None
    manager._pending_config_hash = None
    cases = [
        (
            101,
            kanjicards_module.KanjiUsageInfo(
                reviewed=False,
                first_new_due=5,
                first_new_order=0,
                vocab_occurrences=1,
            ),
            300,
        ),
        (
            102,
            kanjicards_module.KanjiUsageInfo(
                reviewed=False,
                first_new_due=10,
                first_new_order=1,
                vocab_occurrences=5,
            ),
            400,
        ),
        (
            103,
            kanjicards_module.KanjiUsageInfo(
                reviewed=False,
                first_new_due=10,
                first_new_order=2,
                vocab_occurrences=2,
            ),
            100,
        ),
        (
            104,
            kanjicards_module.KanjiUsageInfo(
                reviewed=False,
                first_new_due=10,
                first_new_order=3,
                vocab_occurrences=2,
            ),
            150,
        ),
        (
            105,
            kanjicards_module.KanjiUsageInfo(
                reviewed=False,
                first_new_due=10,
                first_new_order=4,
                vocab_occurrences=2,
            ),
            None,
        ),
    ]

    keys = []
    for card_id, info, frequency in cases:
        key, bucket_id = manager._build_reorder_key(
            "vocab",
            info,
            frequency,
            due_value=0,
            card_id=card_id,
            has_vocab=True,
        )
        assert bucket_id == 1
        keys.append((key, card_id))

    sorted_ids = [card_id for key, card_id in sorted(keys, key=lambda entry: entry[0])]
    assert sorted_ids == [101, 102, 103, 104, 105]
