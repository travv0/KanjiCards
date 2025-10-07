import types

import pytest


def test_add_note_prefers_add_note(kanjicards_module):
    class Coll:
        def __init__(self):
            self.calls = []

        def add_note(self, note, deck_id=None):
            self.calls.append((note, deck_id))
            return True

    col = Coll()
    result = kanjicards_module._add_note(col, "note", 5)
    assert result is True
    assert col.calls == [("note", 5)]


def test_add_note_falls_back_to_legacy(kanjicards_module):
    class Coll:
        def __init__(self):
            self.calls = []

        def addNote(self, note):
            self.calls.append(note)
            return True

    col = Coll()
    result = kanjicards_module._add_note(col, "note", None)
    assert result is True
    assert col.calls == ["note"]


def test_unsuspend_cards_uses_scheduler(kanjicards_module):
    class Sched:
        def __init__(self):
            self.calls = []

        def unsuspend_cards(self, ids):
            self.calls.append(list(ids))

    sched = Sched()
    collection = types.SimpleNamespace(sched=sched)
    kanjicards_module._unsuspend_cards(collection, [1, 2])
    assert sched.calls == [[1, 2]]


def test_unsuspend_cards_updates_db(kanjicards_module, monkeypatch):
    captured = {}

    class DB:
        def execute(self, sql, *params):
            captured["sql"] = sql
            captured["params"] = params

    collection = types.SimpleNamespace(sched=None, db=DB(), usn=lambda: 0)

    monkeypatch.setattr(kanjicards_module, "_db_execute", lambda col, sql, *params, context="": captured.update({"call": (sql, params)}))
    kanjicards_module._unsuspend_cards(collection, [3])
    assert captured["call"][0].startswith("UPDATE cards SET mod")


def test_resuspend_note_cards_uses_scheduler(kanjicards_module, monkeypatch):
    class Sched:
        def __init__(self):
            self.calls = []

        def suspend_cards(self, ids):
            self.calls.append(list(ids))

    note = types.SimpleNamespace(id=9)
    monkeypatch.setattr(kanjicards_module, "_db_all", lambda *args, **kwargs: [(1, 0), (2, -1)])
    collection = types.SimpleNamespace(sched=Sched(), db=types.SimpleNamespace(), usn=lambda: 0)
    count = kanjicards_module._resuspend_note_cards(collection, note)
    assert count == 1
    assert collection.sched.calls == [[1]]


def test_resuspend_note_cards_updates_db(kanjicards_module, monkeypatch):
    calls = {}
    monkeypatch.setattr(kanjicards_module, "_db_all", lambda *args, **kwargs: [(1, 0), (2, 1)])
    monkeypatch.setattr(kanjicards_module, "_db_execute", lambda col, sql, *params, context="": calls.setdefault("sql", sql))
    collection = types.SimpleNamespace(sched=None, db=types.SimpleNamespace(), usn=lambda: 0)
    note = types.SimpleNamespace(id=5)
    count = kanjicards_module._resuspend_note_cards(collection, note)
    assert count == 2
    assert calls["sql"].startswith("UPDATE cards SET mod")


def test_db_all_and_execute_wrap_errors(kanjicards_module, monkeypatch):
    class DB:
        def all(self, sql, *params):
            raise RuntimeError("fail all")

        def execute(self, sql, *params):
            raise RuntimeError("fail execute")

    logs = []
    monkeypatch.setattr(kanjicards_module, "_log_db_error", lambda *args: logs.append(args))
    collection = types.SimpleNamespace(db=DB())

    with pytest.raises(RuntimeError):
        kanjicards_module._db_all(collection, "SELECT 1")
    with pytest.raises(RuntimeError):
        kanjicards_module._db_execute(collection, "UPDATE")
    assert logs and logs[0][0] == "all"


def test_log_db_error_prints(capsys, kanjicards_module):
    kanjicards_module._log_db_error("all", "SQL", (1,), "ctx", RuntimeError("boom"))
    output = capsys.readouterr().out
    assert "db.all failed" in output
    assert "SQL" in output


def test_new_note_and_get_note_fallbacks(kanjicards_module):
    class Coll:
        def __init__(self):
            self._notes = {1: "note"}

        def newNote(self, model):
            return {"model": model}

        def getNote(self, note_id):
            return self._notes[note_id]

    col = Coll()
    assert kanjicards_module._new_note(col, {"id": 1}) == {"model": {"id": 1}}
    assert kanjicards_module._get_note(col, 1) == "note"
