import types

import pytest


class FakeDB:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def all(self, sql, *params):
        self.calls.append((sql, params))
        normalized = " ".join(sql.split()).lower()
        if "select count" in normalized and "from notes" in normalized:
            return [(len(self._rows), 0)]
        if "select distinct cards.did" in normalized:
            deck_ids = sorted({int(row[3]) for row in self._rows})
            return [(deck_id,) for deck_id in deck_ids]
        return list(self._rows)


class FakeDeckManager:
    def __init__(self, decks, configs):
        self._decks = {int(deck_id): dict(deck_data) for deck_id, deck_data in decks.items()}
        self._configs = {int(deck_id): dict(config) for deck_id, config in configs.items()}

    def get(self, did, default=False):
        return dict(self._decks.get(int(did), {}))

    def config_dict_for_deck_id(self, did):
        return dict(self._configs.get(int(did), {}))

    def id_for_name(self, name):
        lowered = name.lower()
        for deck_id, deck in self._decks.items():
            deck_name = str(deck.get("name", "")).lower()
            if deck_name == lowered:
                return deck_id
        return None

    def all_names_and_ids(self):
        return [(deck.get("name", ""), deck_id) for deck_id, deck in self._decks.items()]


class FakeCollection:
    def __init__(self, rows, decks, configs):
        self.db = FakeDB(rows)
        self.decks = FakeDeckManager(decks, configs)


@pytest.fixture
def manager(kanjicards_module):
    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    manager.mw = types.SimpleNamespace()
    manager._debug_enabled = False
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._last_vocab_deck_signature = None
    manager._pending_vocab_sync_marker = None
    manager._pending_vocab_deck_signature = None
    manager._suppress_next_auto_sync = False
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
        use_parent_deck_new_order=True,
        debug_logging=False,
        ignore_suspended_vocab=False,
        known_kanji_interval=21,
        auto_suspend_vocab=False,
        auto_suspend_tag="",
        resuspend_reviewed_low_interval=False,
        low_interval_vocab_tag="",
    )


def test_collect_vocab_usage_tracks_firsts(manager, kanjicards_module):
    gather_due = kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION
    rows = [
        (1, "火火\x1fmeaning", 101, 100, 2, 2, 15),
        (2, "火曜\x1fmeaning", 201, 100, 0, 0, 10),
        (3, "水\x1fmeaning", 301, 100, 0, 0, 20),
    ]
    decks = {
        100: {"id": 100, "name": "Vocab"},
    }
    configs = {
        100: {"new": {"new_per_day": 1, "new_card_gather_priority": gather_due}},
    }
    collection = FakeCollection(rows, decks, configs)
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
    assert fire_info.first_new_due == 0
    assert fire_info.first_new_order == 0
    assert fire_info.first_review_order == 0

    weekday_info = usage["曜"]
    assert weekday_info.reviewed is False
    assert weekday_info.first_new_due == 0
    assert weekday_info.vocab_occurrences == 1

    water_info = usage["水"]
    assert water_info.first_new_due == 1
    assert water_info.first_new_order == 1


@pytest.mark.parametrize(
    ("gather_priority", "expected_day", "expected_order"),
    [
        ("deck", 1, 2),
        ("due", 0, 1),
    ],
)
def test_collect_vocab_usage_respects_parent_gather(manager, kanjicards_module, gather_priority, expected_day, expected_order):
    mode_map = {
        "deck": kanjicards_module.NEW_CARD_GATHER_PRIORITY_DECK,
        "due": kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION,
    }
    priority_value = mode_map[gather_priority]
    rows = [
        (1, "甲\x1fmeaning", 501, 11, 0, 0, 0),
        (2, "田\x1fmeaning", 502, 11, 0, 0, 1),
        (3, "乙\x1fmeaning", 601, 12, 0, 0, 0),
    ]
    decks = {
        10: {"id": 10, "name": "Root"},
        11: {"id": 11, "name": "Root::A"},
        12: {"id": 12, "name": "Root::B"},
    }
    configs = {
        10: {"new": {"new_per_day": 2, "new_card_gather_priority": priority_value}},
        11: {"new": {"new_per_day": 2, "new_card_gather_priority": priority_value}},
        12: {"new": {"new_per_day": 2, "new_card_gather_priority": priority_value}},
    }
    collection = FakeCollection(rows, decks, configs)
    model = {
        "id": 2,
        "name": "Names",
        "flds": [{"name": "Expression"}],
    }
    usage = manager._collect_vocab_usage(collection, [(model, [0])], make_config(kanjicards_module))

    assert usage["甲"].first_new_due == 0
    if gather_priority == "deck":
        assert usage["田"].first_new_due == 0
    else:
        assert usage["田"].first_new_due == 1
    assert usage["乙"].first_new_due == expected_day
    assert usage["乙"].first_new_order == expected_order


def test_collect_vocab_usage_distributes_across_limits(manager, kanjicards_module):
    config = make_config(kanjicards_module)
    config.use_parent_deck_new_order = True
    gather_due = kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION

    parent_id = 100
    deck_a_id = 101
    deck_b_id = 102
    extra_id = 200

    decks = {
        parent_id: {"id": parent_id, "name": "Root"},
        deck_a_id: {"id": deck_a_id, "name": "Root::Alpha"},
        deck_b_id: {"id": deck_b_id, "name": "Root::Beta"},
        extra_id: {"id": extra_id, "name": "Standalone"},
    }
    configs = {
        parent_id: {"new": {"new_per_day": 20, "new_card_gather_priority": gather_due}},
        deck_a_id: {"new": {"new_per_day": 10, "new_card_gather_priority": gather_due}},
        deck_b_id: {"new": {"new_per_day": 10, "new_card_gather_priority": gather_due}},
        extra_id: {"new": {"new_per_day": 1, "new_card_gather_priority": gather_due}},
    }

    rows = []
    next_note_id = 1000
    next_card_id = 5000
    next_char_offset = 0

    def add_card(deck_id: int, due: int) -> str:
        nonlocal next_note_id, next_card_id, next_char_offset
        char = chr(0x4E00 + next_char_offset)
        next_char_offset += 1
        next_note_id += 1
        next_card_id += 1
        fields = f"{char}\x1fmeaning"
        rows.append((next_note_id, fields, next_card_id, deck_id, 0, 0, due))
        return char

    deck_a_chars = [add_card(deck_a_id, idx) for idx in range(30)]
    deck_b_chars = [add_card(deck_b_id, idx) for idx in range(30)]
    extra_chars = [add_card(extra_id, idx) for idx in range(10)]

    collection = FakeCollection(rows, decks, configs)
    model = {"id": 3, "name": "Composite", "flds": [{"name": "Expression"}]}
    usage = manager._collect_vocab_usage(collection, [(model, [0])], config)

    assert all(char in usage for char in deck_a_chars + deck_b_chars + extra_chars)

    for idx, char in enumerate(deck_a_chars):
        info = usage[char]
        assert info.first_new_due == idx // 10
    for idx, char in enumerate(deck_b_chars):
        info = usage[char]
        assert info.first_new_due == idx // 10
    for idx, char in enumerate(extra_chars):
        info = usage[char]
        assert info.first_new_due == idx
        assert info.first_new_order == idx

    parent_day_counts = {}
    for char in deck_a_chars + deck_b_chars:
        day = usage[char].first_new_due
        parent_day_counts.setdefault(day, 0)
        parent_day_counts[day] += 1

    assert parent_day_counts[0] == 20
    assert parent_day_counts[1] == 20


def test_collect_vocab_usage_ignores_zero_parent_limit(manager, kanjicards_module):
    config = make_config(kanjicards_module)
    config.use_parent_deck_new_order = True

    parent_id = 400
    alpha_id = 401
    proper_id = 402

    decks = {
        parent_id: {"id": parent_id, "name": "Root"},
        alpha_id: {"id": alpha_id, "name": "Root::Alpha"},
        proper_id: {"id": proper_id, "name": "Root::Proper"},
    }
    gather_deck = kanjicards_module.NEW_CARD_GATHER_PRIORITY_DECK
    configs = {
        parent_id: {"new": {"new_per_day": 0, "new_card_gather_priority": gather_deck}},
        alpha_id: {"new": {"new_per_day": 40, "new_card_gather_priority": gather_deck}},
        proper_id: {"new": {"new_per_day": 1, "new_card_gather_priority": gather_deck}},
    }

    rows = []
    next_note_id = 10_000
    next_card_id = 50_000

    def add_card(deck_id: int, due: int, code_point: int) -> str:
        nonlocal next_note_id, next_card_id
        char = chr(code_point)
        fields = f"{char}\x1fmeaning"
        rows.append((next_note_id, fields, next_card_id, deck_id, 0, 0, due))
        next_note_id += 1
        next_card_id += 1
        return char

    for idx in range(40):
        add_card(alpha_id, idx, 0x4E00 + idx)

    proper_chars = [add_card(proper_id, idx, 0x4E00 + 100 + idx) for idx in range(10)]
    target_char = proper_chars[5]

    collection = FakeCollection(rows, decks, configs)
    model = {"id": 4, "name": "Composite", "flds": [{"name": "Expression"}]}

    usage = manager._collect_vocab_usage(collection, [(model, [0])], config)
    target_info = usage[target_char]

    assert target_info.first_new_due == 5
    assert target_info.first_new_order == 5

    for idx, char in enumerate(proper_chars):
        info = usage[char]
        assert info.first_new_due == idx
        assert info.first_new_order == idx

    alpha_samples = [chr(0x4E00 + idx) for idx in range(5)]
    for idx, char in enumerate(alpha_samples):
        info = usage[char]
        assert info.first_new_due == 0
        assert info.first_new_order == idx


def test_collect_vocab_usage_includes_auto_suspended_notes(manager, kanjicards_module, monkeypatch):
    config = make_config(kanjicards_module)
    config.ignore_suspended_vocab = True
    config.auto_suspend_tag = "KC-AUTO"
    config.use_parent_deck_new_order = True

    gather_due = kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION
    deck_id = 777
    decks = {deck_id: {"id": deck_id, "name": "Root::Deck"}}
    configs = {
        deck_id: {"new": {"new_per_day": 1, "new_card_gather_priority": gather_due}},
    }

    suspended_auto_id = 1001
    suspended_manual_id = 1002
    active_id = 1003
    rows = [
        (suspended_auto_id, "篭\x1fmeaning", 2001, deck_id, -1, 0, 0),
        (suspended_manual_id, "哉\x1fmeaning", 2002, deck_id, -1, 0, 50),
        (active_id, "庵\x1fmeaning", 2003, deck_id, 0, 0, 1),
    ]

    collection = FakeCollection(rows, decks, configs)

    def fake_active_status(self, _collection, note_ids):
        return {
            suspended_auto_id: False,
            suspended_manual_id: False,
            active_id: True,
            **{
                note_id: True
                for note_id in note_ids
                if note_id not in {suspended_auto_id, suspended_manual_id, active_id}
            },
        }

    manager._load_note_active_status = types.MethodType(fake_active_status, manager)

    tag_map = {
        suspended_auto_id: ["KC-AUTO"],
        suspended_manual_id: ["manual-suspend"],
        active_id: [],
    }

    def fake_get_note(_collection, note_id):
        tags = list(tag_map.get(note_id, []))
        note = types.SimpleNamespace(tags=tags)
        note.flush = lambda: None
        return note

    monkeypatch.setattr(kanjicards_module, "_get_note", fake_get_note)

    model = {"id": 55, "name": "Composite", "flds": [{"name": "Expression"}]}
    usage = manager._collect_vocab_usage(collection, [(model, [0])], config)

    assert "篭" in usage
    assert usage["篭"].first_new_due == 0
    assert usage["篭"].first_new_order == 0
    assert "哉" not in usage
    assert usage["庵"].first_new_due == 1


def test_collect_vocab_usage_marks_new_even_with_review(manager, kanjicards_module):
    config = make_config(kanjicards_module)
    config.use_parent_deck_new_order = True

    deck_id = 8888
    decks = {deck_id: {"id": deck_id, "name": "Root"}}
    gather_due = kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION
    configs = {
        deck_id: {"new": {"new_per_day": 10, "new_card_gather_priority": gather_due}},
    }

    rows = [
        (5001, "懺悔\x1fmeaning", 9001, deck_id, 0, 0, 0),
        (5001, "懺悔\x1fmeaning", 9002, deck_id, 0, 2, 400),
    ]

    collection = FakeCollection(rows, decks, configs)
    model = {"id": 42, "name": "Sentences", "flds": [{"name": "Expression"}]}

    usage = manager._collect_vocab_usage(collection, [(model, [0])], config)
    info = usage["懺"]

    assert info.reviewed is True
    assert info.has_new_card is True
    assert info.first_new_due == 0


def test_collect_vocab_usage_updates_when_limits_change(manager, kanjicards_module):
    config = make_config(kanjicards_module)
    config.use_parent_deck_new_order = True
    gather_due = kanjicards_module.NEW_CARD_GATHER_PRIORITY_LOWEST_POSITION

    deck_id = 501
    decks = {deck_id: {"id": deck_id, "name": "Independent"}}
    configs = {deck_id: {"new": {"new_per_day": 1, "new_card_gather_priority": gather_due}}}

    rows = []
    next_note = 2000
    next_card = 9000
    characters = []
    for idx in range(5):
        char = chr(0x4E00 + idx)
        characters.append(char)
        next_note += 1
        next_card += 1
        rows.append((next_note, f"{char}meaning", next_card, deck_id, 0, 0, idx))

    collection = FakeCollection(rows, decks, configs)
    model = {"id": 99, "name": "Single", "flds": [{"name": "Expression"}]}

    usage_initial = manager._collect_vocab_usage(collection, [(model, [0])], config)
    baseline = [usage_initial[ch].first_new_due for ch in characters]
    assert baseline[0] == 0
    assert baseline[1] == 1

    configs[deck_id]["new"]["new_per_day"] = 30
    usage_updated = manager._collect_vocab_usage(collection, [(model, [0])], config)
    updated = [usage_updated[ch].first_new_due for ch in characters]
    assert updated[0] == 0
    assert updated[1] == 0
    assert all(day == 0 for day in updated)
