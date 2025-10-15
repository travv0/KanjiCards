import sys
import types
from pathlib import Path
from typing import Optional

import pytest


class Hook:
    def __init__(self) -> None:
        self.callbacks = []

    def append(self, callback) -> None:
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def remove(self, callback) -> None:
        if callback in self.callbacks:
            self.callbacks.remove(callback)


class FakeSignal:
    def __init__(self) -> None:
        self.connected = []

    def connect(self, callback) -> None:
        self.connected.append(callback)


class FakeAction:
    def __init__(self, label: str) -> None:
        self.label = label
        self.triggered = FakeSignal()


class FakeMenu:
    def __init__(self) -> None:
        self.actions = []

    def addAction(self, label: str):
        action = FakeAction(label)
        self.actions.append(action)
        return action


class FakeToolbar:
    def __init__(self) -> None:
        self.link_handlers = {}

    def create_link(self, cmd: str, label: str, func, tip: str | None = None, id: str | None = None) -> str:
        self.link_handlers[cmd] = func
        id_attr = f'id="{id}"' if id else ""
        return f'<a {id_attr} data-cmd="{cmd}">{label}</a>'


class FakeAddonManager:
    def __init__(self, addons_folder: str) -> None:
        self._addons_folder = addons_folder
        self.config_actions = {}
        self.written_configs = {}

    def addonFromModule(self, module_name: str) -> str:
        return "KanjiCards"

    def addonsFolder(self) -> str:
        return self._addons_folder

    def setConfigAction(self, module_name: str, action) -> None:
        self.config_actions[module_name] = action

    def getConfig(self, module_name: str) -> dict:
        return {}

    def writeConfig(self, module_name: str, data: dict) -> None:
        self.written_configs[module_name] = data


class FakeProgress:
    def __init__(self) -> None:
        self.started = False
        self.finished = False
        self.updates = []
        self.busy_values = []

    def start(self, **kwargs) -> None:
        self.started = True

    def finish(self) -> None:
        self.finished = True

    def update(self, **kwargs) -> None:
        self.updates.append(kwargs)

    def busy(self) -> bool:
        if self.busy_values:
            return self.busy_values.pop(0)
        return False


class FakeTaskman:
    def __init__(self) -> None:
        self.calls = []

    def run_on_main(self, callback) -> None:
        self.calls.append(callback)
        callback()


class FakeUndoCollection:
    def __init__(self) -> None:
        self.entries: list[dict[str, object]] = []
        self.merge_calls: list[int] = []
        self._next_id = 1

    def add_custom_undo_entry(self, name: str) -> int:
        entry_id = self._next_id
        self._next_id += 1
        self.entries.append({"id": entry_id, "name": name})
        return entry_id

    def add_step(self, name: str) -> int:
        entry_id = self._next_id
        self._next_id += 1
        self.entries.append({"id": entry_id, "name": name})
        return entry_id

    def merge_undo_entries(self, target: int) -> None:
        self.merge_calls.append(target)
        while self.entries and self.entries[-1]["id"] != target:
            self.entries.pop()

    def undo_status(self):
        if not self.entries:
            return types.SimpleNamespace(last_step=None)
        return types.SimpleNamespace(last_step=self.entries[-1]["id"])


class FakeMainWindow:
    def __init__(self, base_dir: Path) -> None:
        progress = FakeProgress()
        self.form = types.SimpleNamespace(menuTools=FakeMenu())
        self.progress = progress
        self.taskman = FakeTaskman()
        self.pm = types.SimpleNamespace(profileFolder=lambda: str(base_dir))
        self.addonManager = FakeAddonManager(str(base_dir / "addons"))
        self._checkpoints = []
        self._reset_calls = 0
        self.col = FakeUndoCollection()

    def checkpoint(self, name: str) -> None:
        self._checkpoints.append(name)

    def reset(self) -> None:
        self._reset_calls += 1


@pytest.fixture
def manager_with_mw(kanjicards_module, tmp_path, monkeypatch):
    hooks = types.SimpleNamespace(
        profile_did_open=Hook(),
        main_window_did_init=Hook(),
        reviewer_did_answer_card=Hook(),
        reviewer_did_show_question=Hook(),
        sync_did_finish=Hook(),
        sync_will_start=Hook(),
    )
    monkeypatch.setattr(kanjicards_module, "gui_hooks", hooks)
    mw = FakeMainWindow(tmp_path)
    monkeypatch.setattr(kanjicards_module, "mw", mw)
    manager = kanjicards_module.KanjiVocabRecalcManager()
    yield manager, mw, hooks


def test_manager_init_wires_menu_and_hooks(manager_with_mw, kanjicards_module):
    manager, mw, hooks = manager_with_mw
    labels = [action.label for action in mw.form.menuTools.actions]
    assert "KanjiCards Recalc" in labels
    assert "KanjiCards Settings" in labels
    assert manager._on_reviewer_did_show_question in hooks.reviewer_did_show_question.callbacks
    assert manager._on_reviewer_did_answer_card in hooks.reviewer_did_answer_card.callbacks
    assert manager._on_sync_event in hooks.sync_did_finish.callbacks or manager._on_sync_event in hooks.sync_will_start.callbacks
    assert kanjicards_module.__name__ in mw.addonManager.config_actions


def test_manager_init_without_registered_addon(monkeypatch, kanjicards_module, tmp_path):
    hooks = types.SimpleNamespace(
        profile_did_open=Hook(),
        main_window_did_init=Hook(),
        reviewer_did_answer_card=Hook(),
        reviewer_did_show_question=Hook(),
        sync_did_finish=Hook(),
        sync_will_start=Hook(),
    )
    monkeypatch.setattr(kanjicards_module, "gui_hooks", hooks)
    mw = FakeMainWindow(tmp_path)
    mw.addonManager = types.SimpleNamespace(
        addonFromModule=lambda name: "",
        addonsFolder=lambda: str(tmp_path / "addons"),
        setConfigAction=lambda *args, **kwargs: None,
        getConfig=lambda name: {},
        writeConfig=lambda name, data: None,
    )
    monkeypatch.setattr(kanjicards_module, "mw", mw)
    manager = kanjicards_module.KanjiVocabRecalcManager()
    assert Path(manager.addon_dir) == Path(kanjicards_module.__file__).parent


def test_toolbar_link_added_without_prioritysieve(manager_with_profile, monkeypatch):
    for key in list(sys.modules):
        if key.startswith("prioritysieve"):
            monkeypatch.delitem(sys.modules, key, raising=False)

    toolbar = FakeToolbar()
    links: list[str] = []

    manager_with_profile._on_top_toolbar_init_links(links, toolbar)

    assert any('id="kanjicards_recalc_toolbar"' in link for link in links)
    assert any(">Recalc<" in link for link in links)

    calls: list[str] = []
    manager_with_profile.run_recalc = lambda: calls.append("kanjicards")  # type: ignore[assignment]

    manager_with_profile._on_toolbar_did_redraw(toolbar)
    handler = toolbar.link_handlers.get("kanjicards_recalc")
    assert callable(handler)
    handler()

    assert calls == ["kanjicards"]


def test_toolbar_skips_when_prioritysieve_installed(manager_with_profile, monkeypatch, kanjicards_module):
    events: list[str] = []

    monkeypatch.setitem(sys.modules, "prioritysieve", types.ModuleType("prioritysieve"))
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc", types.ModuleType("prioritysieve.recalc"))

    class FakeRecalcMainModule(types.ModuleType):
        def __init__(self) -> None:
            super().__init__("prioritysieve.recalc.recalc_main")
            self._followup_sync_callback = None

        def set_followup_sync_callback(self, callback):
            self._followup_sync_callback = callback

        def recalc(self):
            events.append("priority_recalc")
            if callable(self._followup_sync_callback):
                callback = self._followup_sync_callback
                self._followup_sync_callback = None
                callback()

    fake_module = FakeRecalcMainModule()
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc.recalc_main", fake_module)

    manager_with_profile.run_recalc = lambda: events.append("kanjicards")  # type: ignore[assignment]

    toolbar = FakeToolbar()

    link = toolbar.create_link(
        kanjicards_module.PRIORITYSIEVE_TOOLBAR_CMD,
        "PS Recalc",
        fake_module.recalc,
    )
    links = [link]

    manager_with_profile._on_top_toolbar_init_links(links, toolbar)

    assert not any('id="kanjicards_recalc_toolbar"' in link for link in links)
    assert not any(">Recalc<" in link for link in links)

    manager_with_profile._on_toolbar_did_redraw(toolbar)

    ps_handler = toolbar.link_handlers[kanjicards_module.PRIORITYSIEVE_TOOLBAR_CMD]

    ps_handler()
    assert events == ["priority_recalc"]

    assert kanjicards_module.KANJICARDS_TOOLBAR_CMD not in toolbar.link_handlers


def test_prioritysieve_recalc_runs_kanjicards_afterwards(manager_with_profile, monkeypatch):
    events: list[str] = []

    class FakeRecalcMainModule(types.ModuleType):
        def __init__(self) -> None:
            super().__init__("prioritysieve.recalc.recalc_main")
            self._followup_sync_callback = None

        def set_followup_sync_callback(self, callback):
            self._followup_sync_callback = callback

        def recalc(self):
            events.append("priority_recalc")
            if self._followup_sync_callback is not None:
                callback = self._followup_sync_callback
                self._followup_sync_callback = None
                callback()

    fake_module = FakeRecalcMainModule()

    monkeypatch.setitem(sys.modules, "prioritysieve", types.ModuleType("prioritysieve"))
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc", types.ModuleType("prioritysieve.recalc"))
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc.recalc_main", fake_module)

    def previous_callback():
        events.append("priority_followup")

    fake_module._followup_sync_callback = previous_callback

    manager_with_profile.mw.taskman = FakeTaskman()

    manager_with_profile.run_after_sync = lambda *args, **kwargs: events.append("kanjicards")  # type: ignore[assignment]
    manager_with_profile._prioritysieve_waiting_post_sync = True

    manager_with_profile._maybe_wrap_prioritysieve_recalc(fake_module)

    assert getattr(fake_module, "_kanjicards_recalc_wrapper_installed", False) is True
    assert manager_with_profile._prioritysieve_recalc_wrapped is True

    fake_module.recalc()

    assert events == ["priority_recalc", "priority_followup", "kanjicards"]


def test_prioritysieve_recalc_skips_kanjicards_when_not_waiting(manager_with_profile, monkeypatch):
    events: list[str] = []

    class FakeRecalcMainModule(types.ModuleType):
        def __init__(self) -> None:
            super().__init__("prioritysieve.recalc.recalc_main")
            self._followup_sync_callback = None

        def set_followup_sync_callback(self, callback):
            self._followup_sync_callback = callback

        def recalc(self):
            events.append("priority_recalc")
            if self._followup_sync_callback is not None:
                callback = self._followup_sync_callback
                self._followup_sync_callback = None
                callback()

    fake_module = FakeRecalcMainModule()

    monkeypatch.setitem(sys.modules, "prioritysieve", types.ModuleType("prioritysieve"))
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc", types.ModuleType("prioritysieve.recalc"))
    monkeypatch.setitem(sys.modules, "prioritysieve.recalc.recalc_main", fake_module)

    manager_with_profile.mw.taskman = FakeTaskman()

    manager_with_profile.run_after_sync = lambda *args, **kwargs: events.append("kanjicards")  # type: ignore[assignment]
    manager_with_profile._prioritysieve_waiting_post_sync = False

    manager_with_profile._maybe_wrap_prioritysieve_recalc(fake_module)

    fake_module.recalc()

    assert events == ["priority_recalc"]
def test_show_settings_uses_dialog(manager_with_profile, kanjicards_module, monkeypatch):
    recorded = {}

    class DummyDialog:
        def __init__(self, manager, cfg):
            recorded["cfg"] = cfg

        def exec(self):
            recorded["exec"] = True

    monkeypatch.setattr(kanjicards_module, "KanjiVocabRecalcSettingsDialog", DummyDialog)
    manager_with_profile.load_config = lambda: {"existing_tag": "x"}  # type: ignore[assignment]
    manager_with_profile.show_settings()
    assert recorded["exec"] is True


def test_run_recalc_success_and_failure(manager_with_profile, kanjicards_module, monkeypatch, tmp_path):
    mw = FakeMainWindow(tmp_path)
    manager_with_profile.mw = mw
    manager_with_profile.addon_dir = str(tmp_path)
    stats_called = {}
    monkeypatch.setattr(manager_with_profile, "_notify_summary", lambda stats: stats_called.setdefault("stats", stats))
    def fake_recalc(**kwargs):
        mw.col.add_step("note update")
        manager_with_profile._merge_recalc_undo_step(mw.col)
        mw.col.add_step("card reorder")
        manager_with_profile._merge_recalc_undo_step(mw.col)
        return {"created": 1}

    manager_with_profile._recalc_internal = fake_recalc  # type: ignore[assignment]
    cfg = manager_with_profile._config_from_raw(
        {
            "kanji_note_type": {"name": "Kanji", "fields": {}},
            "vocab_note_types": [],
        }
    )
    manager_with_profile.load_config = lambda: cfg  # type: ignore[assignment]

    result = manager_with_profile.run_recalc()
    assert result["created"] == 1
    assert stats_called["stats"]["created"] == 1
    assert mw.progress.finished is True
    assert mw._reset_calls == 1
    assert [entry["name"] for entry in mw.col.entries] == ["KanjiCards Recalc"]
    assert mw.col.merge_calls == [1, 1]
    assert mw._checkpoints == []

    def failing_recalc(**kwargs):
        mw.col.add_step("partial update")
        manager_with_profile._merge_recalc_undo_step(mw.col)
        raise RuntimeError("boom")

    manager_with_profile._recalc_internal = failing_recalc  # type: ignore[assignment]
    called = {}
    monkeypatch.setattr(kanjicards_module, "show_critical", lambda message: called.setdefault("message", message))
    assert manager_with_profile.run_recalc() is None
    assert "boom" in called["message"]
    assert [entry["name"] for entry in mw.col.entries] == ["KanjiCards Recalc", "KanjiCards Recalc"]
    assert mw.col.merge_calls == [1, 1, 4]


def test_merge_recalc_undo_step_ignores_without_active_target(manager_with_profile, kanjicards_module):
    manager = manager_with_profile

    class TrackingCollection:
        def __init__(self) -> None:
            self.merge_calls = []
            self.custom_entries = []

        def merge_undo_entries(self, target: int):
            self.merge_calls.append(target)

        def add_custom_undo_entry(self, name: str) -> int:
            self.custom_entries.append(name)
            return 42

        def undo_status(self):
            return types.SimpleNamespace(last_step=1)

    collection = TrackingCollection()
    manager._active_recalc_undo = None
    manager._pending_undo_retry = True
    manager._merge_recalc_undo_step(collection)  # type: ignore[arg-type]
    assert collection.merge_calls == []
    assert manager._pending_undo_retry is False


def test_merge_recalc_undo_step_recovers_missing_target(manager_with_profile, kanjicards_module):
    manager = manager_with_profile

    class FailingUndoCollection:
        def __init__(self) -> None:
            self.entries = [
                {"id": 1, "name": "KanjiCards Recalc", "has_changes": True},
                {"id": 2, "name": "Suspend", "has_changes": True},
            ]
            self._next_id = 3
            self._fail_once = True
            self.queue = -1

        def add_custom_undo_entry(self, name: str) -> int:
            entry_id = self._next_id
            self._next_id += 1
            self.entries.append({"id": entry_id, "name": name, "has_changes": False})
            return entry_id

        def undo_status(self):
            if not self.entries:
                return types.SimpleNamespace(last_step=None)
            return types.SimpleNamespace(last_step=self.entries[-1]["id"])

        def merge_undo_entries(self, target: int):
            if self._fail_once:
                self._fail_once = False
                raise RuntimeError("target undo op not found")
            while self.entries and self.entries[-1]["id"] != target:
                self.entries.pop()
            if self.entries:
                self.entries[-1]["has_changes"] = True
            return types.SimpleNamespace(ListFields=lambda: [1])

        def undo(self):
            if not self.entries:
                raise RuntimeError("empty undo stack")
            entry = self.entries.pop()
            if entry["name"].lower().startswith("suspend"):
                self.queue = 0
            return types.SimpleNamespace(ListFields=lambda: [1])

        def set_suspended(self, ids, suspended: bool):
            entry_id = self._next_id
            self._next_id += 1
            label = "Suspend" if suspended else "Unsuspend"
            self.entries.append({"id": entry_id, "name": label, "has_changes": bool(suspended)})
            self.queue = -1 if suspended else 0

    collection = FailingUndoCollection()
    manager._active_recalc_undo = 1
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(
        card_ids=[702],
        tag="NeedsSuspend",
        note_ids=[],
    )

    manager._merge_recalc_undo_step(collection)  # type: ignore[arg-type]

    assert collection.entries[-1]["name"] == "KanjiCards Recalc"
    assert not any(entry["name"].lower().startswith("suspend") for entry in collection.entries)


def test_merge_recalc_undo_step_replay_keeps_single_undo(kanjicards_module):
    manager = kanjicards_module.KanjiVocabRecalcManager.__new__(kanjicards_module.KanjiVocabRecalcManager)
    manager._debug_enabled = False
    manager._pending_suspend_retry = None
    manager._pending_undo_retry = False

    class ReplayCollection:
        def __init__(self) -> None:
            self.entries = [{"id": 1, "name": "KanjiCards Recalc", "has_changes": False}]
            self.next_id = 2
            self.fail_once = True
            self.card_queue = 0
            self.pending_changes = False

        def add_custom_undo_entry(self, name: str) -> int:
            entry_id = self.next_id
            self.next_id += 1
            self.entries.append({"id": entry_id, "name": name, "has_changes": False})
            return entry_id

        def undo_status(self):
            if not self.entries:
                return types.SimpleNamespace(last_step=None, undo="")
            last = self.entries[-1]
            return types.SimpleNamespace(last_step=last["id"], undo=last["name"])

        def merge_undo_entries(self, target: int):
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("target undo op not found")
            while self.entries and self.entries[-1]["id"] != target:
                self.entries.pop()
            if self.entries:
                self.entries[-1]["has_changes"] = self.entries[-1]["has_changes"] or self.pending_changes
            self.pending_changes = False
            return types.SimpleNamespace(ListFields=lambda: [1])

        def undo(self):
            if not self.entries:
                raise RuntimeError("empty undo stack")
            entry = self.entries.pop()
            if entry.get("has_changes"):
                self.card_queue = 0
            return types.SimpleNamespace(ListFields=lambda: [1], undoed_op=entry["name"])

        def set_suspended(self, ids, suspended: bool):
            self.pending_changes = bool(suspended)
            self.card_queue = -1 if suspended else 0
            if suspended and self.entries:
                self.entries[-1]["has_changes"] = True

    collection = ReplayCollection()
    manager._active_recalc_undo = 1
    collection.set_suspended([702], True)
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(
        card_ids=[702],
        tag="NeedsSuspend",
        note_ids=[],
    )

    manager._merge_recalc_undo_step(collection)

    assert collection.undo_status().undo == "KanjiCards Recalc"
    assert collection.card_queue == -1

    collection.undo()

    assert collection.card_queue == 0


def test_merge_recalc_undo_step_restores_tag_on_retry(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile

    class TagNote:
        def __init__(self, note_id: int) -> None:
            self.id = note_id
            self.tags: list[str] = []
            self.flush_count = 0

        def add_tag(self, tag: str) -> None:
            if tag not in self.tags:
                self.tags.append(tag)

        addTag = add_tag

    class RetryTagCollection:
        def __init__(self, note: TagNote) -> None:
            self.note = note
            self.entries = [
                {"id": 1, "name": "KanjiCards Recalc", "has_changes": True},
                {"id": 2, "name": "Suspend", "has_changes": True},
            ]
            self._next_id = 3
            self._fail_once = True
            self.undo_calls = 0
            self.merge_calls: list[int] = []

        def add_custom_undo_entry(self, name: str) -> int:
            entry_id = self._next_id
            self._next_id += 1
            self.entries.append({"id": entry_id, "name": name, "has_changes": False})
            return entry_id

        def undo_status(self):
            if not self.entries:
                return types.SimpleNamespace(last_step=None, undo="")
            last = self.entries[-1]
            return types.SimpleNamespace(last_step=last["id"], undo=last["name"])

        def merge_undo_entries(self, target: int):
            self.merge_calls.append(target)
            if self._fail_once:
                self._fail_once = False
                raise RuntimeError("target undo op not found")
            while self.entries and self.entries[-1]["id"] != target:
                self.entries.pop()
            if self.entries:
                self.entries[-1]["has_changes"] = True
            return types.SimpleNamespace(ListFields=lambda: [1])

        def undo(self):
            if not self.entries:
                raise RuntimeError("empty undo stack")
            entry = self.entries.pop()
            if entry["name"].lower().startswith("suspend"):
                self.note.tags.clear()
            self.undo_calls += 1
            return types.SimpleNamespace(ListFields=lambda: [1])

        def set_suspended(self, ids, suspended: bool):
            label = "Suspend" if suspended else "Unsuspend"
            self.entries.append({"id": self._next_id, "name": label, "has_changes": bool(suspended)})
            self._next_id += 1
            return len(ids)

        def update_note(self, note: TagNote):
            note.flush_count += 1
            return types.SimpleNamespace(ListFields=lambda: [1])

        def get_note(self, note_id: int):
            if note_id != self.note.id:
                raise AssertionError("unexpected note id")
            return self.note

    note = TagNote(77)
    collection = RetryTagCollection(note)

    monkeypatch.setattr(
        kanjicards_module,
        "_try_set_suspended_state",
        lambda _collection, ids, suspended: len(ids),
    )

    manager._active_recalc_undo = 1
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(
        card_ids=[702],
        tag="NeedsSuspend",
        note_ids=[77],
    )

    manager._merge_recalc_undo_step(collection)  # type: ignore[arg-type]

    assert "NeedsSuspend" in note.tags
    assert note.flush_count == 1
    assert manager._active_recalc_undo is not None


def test_merge_recalc_undo_step_handles_retry_without_payload(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._active_recalc_undo = 4
    manager._pending_suspend_retry = None

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        if log_prefix == "step":
            manager._pending_undo_retry = True
        return target

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)

    class BareCollection:
        def undo_status(self):
            return types.SimpleNamespace(last_step=4, undo="KanjiCards Recalc")

    manager._merge_recalc_undo_step(BareCollection())  # type: ignore[arg-type]
    assert manager._active_recalc_undo == 4


def test_merge_recalc_undo_step_merges_base_target(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._active_recalc_undo = 5
    manager._pending_suspend_retry = None
    manager._pending_undo_retry = False
    calls: list[tuple[Optional[int], str, bool]] = []

    def fake_coalesce(_collection, target, *, log_prefix, force_merge=False, **kwargs):
        calls.append((target, log_prefix, force_merge))
        if log_prefix == "step":
            return 9
        if log_prefix == "step/base":
            return 6
        return target

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)

    class DummyCollection:
        def undo_status(self):
            return types.SimpleNamespace(last_step=6, undo="KanjiCards Recalc")

    manager._merge_recalc_undo_step(DummyCollection())  # type: ignore[arg-type]

    assert manager._active_recalc_undo == 6
    assert ("step", False) in [(prefix, force) for _, prefix, force in calls]
    assert ("step/base", True) in [(prefix, force) for _, prefix, force in calls]


def test_merge_recalc_undo_step_handles_none_collection(manager_with_profile):
    manager = manager_with_profile
    manager._active_recalc_undo = 8
    manager._merge_recalc_undo_step(None)
    assert manager._active_recalc_undo == 8


def test_merge_recalc_undo_step_handles_missing_last_step(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._active_recalc_undo = 12
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(card_ids=[401], tag="NeedsSuspend", note_ids=[])

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        manager._pending_undo_retry = True
        return target

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)
    monkeypatch.setattr(manager, "_get_last_undo_step", lambda _collection: None)

    class DummyCollection:
        def undo_status(self):
            return types.SimpleNamespace(last_step=None, undo="")

    manager._merge_recalc_undo_step(DummyCollection())  # type: ignore[arg-type]
    assert manager._active_recalc_undo == 12


def test_merge_recalc_undo_step_handles_undo_failure(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._active_recalc_undo = 21
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(card_ids=[501], tag="NeedsSuspend", note_ids=[])
    monkeypatch.setattr(manager, "_debug", lambda *args, **kwargs: None)

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        manager._pending_undo_retry = True
        return target

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)
    monkeypatch.setattr(manager, "_get_last_undo_step", lambda _collection: 99)

    class FailingUndo:
        def undo_status(self):
            return types.SimpleNamespace(last_step=99, undo="Suspend")

        def undo(self):
            raise RuntimeError("undo failed")

    manager._merge_recalc_undo_step(FailingUndo())  # type: ignore[arg-type]
    assert manager._active_recalc_undo == 21


def test_merge_recalc_undo_step_returns_when_target_none(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._active_recalc_undo = 31
    manager._pending_suspend_retry = None

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        return None

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)

    class DummyCollection:
        def undo_status(self):
            return types.SimpleNamespace(last_step=0, undo="")

    manager._merge_recalc_undo_step(DummyCollection())  # type: ignore[arg-type]
    assert manager._active_recalc_undo == 31


def test_merge_recalc_undo_step_preserves_prior_history_on_retry(manager_with_profile, kanjicards_module, monkeypatch):
    manager = manager_with_profile
    manager._debug_enabled = False

    class ClearingCollection:
        def __init__(self) -> None:
            self.entries = [
                {"id": 1, "name": "Manual Edit"},
                {"id": 2, "name": "Template Change"},
            ]
            self._next_id = 3

        def add_custom_undo_entry(self, name: str) -> int:
            entry_id = self._next_id
            self._next_id += 1
            self.entries.append({"id": entry_id, "name": name})
            return entry_id

        def undo_status(self):
            if not self.entries:
                return types.SimpleNamespace(last_step=None, undo="")
            last = self.entries[-1]
            return types.SimpleNamespace(last_step=last["id"], undo=last["name"])

        def undo(self):
            if not self.entries:
                raise RuntimeError("empty undo stack")
            removed = self.entries.pop()
            return types.SimpleNamespace(ListFields=lambda: [1], undoed_op=removed["name"])

        def set_suspended(self, ids, suspended: bool):
            return len(ids)

    collection = ClearingCollection()
    start_target = manager._start_recalc_undo_entry(collection)
    assert start_target is not None
    collection.add_custom_undo_entry("Suspend")
    manager._pending_suspend_retry = kanjicards_module.PendingSuspendRetry(
        card_ids=[702],
        tag="NeedsSuspend",
        note_ids=[],
    )

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        manager._pending_undo_retry = True
        return None

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)
    monkeypatch.setattr(kanjicards_module, "_try_set_suspended_state", lambda *_args, **_kwargs: 0)

    manager._merge_recalc_undo_step(collection)

    names = [entry["name"] for entry in collection.entries]
    assert "Manual Edit" in names
    assert "Template Change" in names


def test_restore_suspend_tags_for_retry_skips_blank_tag(manager_with_profile, kanjicards_module):
    manager = manager_with_profile
    result = manager._restore_suspend_tags_for_retry(
        collection=types.SimpleNamespace(),
        payload=kanjicards_module.PendingSuspendRetry(card_ids=[], tag="   ", note_ids=[1]),
    )
    assert result == 0


def test_ensure_recalc_undo_entry_returns_target_when_collection_missing(manager_with_profile):
    manager = manager_with_profile
    assert manager._ensure_recalc_undo_entry(None, 42, log_prefix="unit") == 42


def test_ensure_recalc_undo_entry_handles_missing_getter(manager_with_profile):
    manager = manager_with_profile

    class NoUndoStatus:
        pass

    assert manager._ensure_recalc_undo_entry(NoUndoStatus(), 7, log_prefix="unit") == 7


def test_ensure_recalc_undo_entry_skips_when_last_step_invalid(manager_with_profile):
    manager = manager_with_profile

    class ZeroUndoStatus:
        def undo_status(self):
            return types.SimpleNamespace(last_step=0, undo="")

    assert manager._ensure_recalc_undo_entry(ZeroUndoStatus(), 9, log_prefix="unit") == 9


def test_ensure_recalc_undo_entry_returns_when_step_matches(manager_with_profile):
    manager = manager_with_profile

    class MatchingUndoStatus:
        def undo_status(self):
            return types.SimpleNamespace(last_step=15, undo="")

    assert manager._ensure_recalc_undo_entry(MatchingUndoStatus(), 15, log_prefix="unit") == 15


def test_ensure_recalc_undo_entry_uses_existing_text(manager_with_profile):
    manager = manager_with_profile

    class TextUndoStatus:
        def undo_status(self):
            return types.SimpleNamespace(last_step=18, undo="KanjiCards Recalc - extra")

    assert manager._ensure_recalc_undo_entry(TextUndoStatus(), 4, log_prefix="unit") == 18


def test_ensure_recalc_undo_entry_creates_fallback_entry(manager_with_profile, monkeypatch):
    manager = manager_with_profile
    collection = FakeUndoCollection()
    collection.add_custom_undo_entry("Initial")
    manager._active_recalc_undo = 100

    def fake_coalesce(_collection, target, *, log_prefix, **kwargs):
        return 55

    monkeypatch.setattr(manager, "_coalesce_undo_stack", fake_coalesce)

    result = manager._ensure_recalc_undo_entry(collection, 3, log_prefix="unit")
    assert result == 55
    assert manager._active_recalc_undo == collection.entries[-1]["id"]


def test_ensure_recalc_undo_entry_handles_status_exception(manager_with_profile, monkeypatch):
    manager = manager_with_profile

    class FailingUndoStatus:
        def undo_status(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(manager, "_debug", lambda *args, **kwargs: None)
    assert manager._ensure_recalc_undo_entry(FailingUndoStatus(), 8, log_prefix="unit") == 8


def test_ensure_recalc_undo_entry_uses_lastStep_attribute(manager_with_profile):
    manager = manager_with_profile

    class LegacyUndoStatus:
        def undo_status(self):
            return types.SimpleNamespace(last_step=None, lastStep=23, undo="KanjiCards Recalc legacy")

    assert manager._ensure_recalc_undo_entry(LegacyUndoStatus(), 4, log_prefix="unit") == 23


def test_ensure_recalc_undo_entry_handles_missing_custom_entry(manager_with_profile, monkeypatch):
    manager = manager_with_profile

    class BasicUndoStatus:
        def __init__(self) -> None:
            self.called = False

        def undo_status(self):
            return types.SimpleNamespace(last_step=9, undo="")

    status = BasicUndoStatus()
    collection = types.SimpleNamespace(undo_status=status.undo_status)
    monkeypatch.setattr(manager, "_create_custom_undo_entry", lambda *_args, **_kwargs: None)
    assert manager._ensure_recalc_undo_entry(collection, 2, log_prefix="unit") == 2


def test_finalize_recalc_undo_wraps_remaining_steps(kanjicards_module):
    manager = kanjicards_module.KanjiVocabRecalcManager.__new__(kanjicards_module.KanjiVocabRecalcManager)
    manager._debug_enabled = False

    class LaggyUndoCollection:
        def __init__(self) -> None:
            self.entries = [
                {"id": 1, "name": "KanjiCards Recalc"},
                {"id": 2, "name": "Suspend"},
            ]
            self._next_id = 3
            self.merge_calls: list[int] = []
            self._attempts = 0

        def add_custom_undo_entry(self, name: str) -> int:
            entry_id = self._next_id
            self._next_id += 1
            self.entries.append({"id": entry_id, "name": name})
            return entry_id

        def undo_status(self):
            if not self.entries:
                return types.SimpleNamespace(last_step=None, undo="")
            last = self.entries[-1]
            return types.SimpleNamespace(last_step=last["id"], undo=last["name"])

        def merge_undo_entries(self, target: int):
            self.merge_calls.append(target)
            if self._attempts == 0:
                self._attempts += 1
                return types.SimpleNamespace(ListFields=lambda: [])
            self._attempts += 1
            if len(self.entries) >= 2:
                self.entries.pop(-2)
            if self.entries:
                self.entries[-1]["name"] = "KanjiCards Recalc"
            return types.SimpleNamespace(ListFields=lambda: [1])

    collection = LaggyUndoCollection()
    manager.mw = types.SimpleNamespace(col=collection)

    manager._finalize_recalc_undo(collection, 1)

    assert collection.entries
    assert collection.entries[-1]["name"] == "KanjiCards Recalc"
    assert not any(entry["name"].lower().startswith("suspend") for entry in collection.entries)


def test_start_recalc_undo_entry_accepts_proto(manager_with_profile, monkeypatch):
    mw = types.SimpleNamespace(col=FakeUndoCollection())
    manager_with_profile.mw = mw
    proto = types.SimpleNamespace(id=99)
    monkeypatch.setattr(mw.col, "add_custom_undo_entry", lambda name: proto)

    undo_id = manager_with_profile._start_recalc_undo_entry(mw.col)

    assert undo_id == proto.id
    assert manager_with_profile._active_recalc_undo == proto.id


def test_on_sync_event_handles_busy_and_followup(manager_with_profile, kanjicards_module, monkeypatch, tmp_path):
    mw = FakeMainWindow(tmp_path)
    manager_with_profile.mw = mw
    manager_with_profile._suppress_next_auto_sync = False
    cfg = manager_with_profile._config_from_raw(
        {
            "kanji_note_type": {"name": "Kanji", "fields": {}},
            "vocab_note_types": [],
            "auto_run_on_sync": True,
        }
    )
    manager_with_profile.load_config = lambda: cfg  # type: ignore[assignment]
    manager_with_profile._stats_warrant_sync = lambda stats: True  # type: ignore[assignment]
    manager_with_profile.run_recalc = lambda: {"created": 1}  # type: ignore[assignment]
    manager_with_profile._trigger_followup_sync = lambda: True  # type: ignore[assignment]
    manager_with_profile._have_vocab_notes_changed = lambda collection, cfg: True  # type: ignore[assignment]
    mw.col = object()
    mw.progress.busy_values = [True, False]

    delays = []

    def fake_single_shot(delay, callback):
        delays.append(delay)
        callback()

    monkeypatch.setattr(kanjicards_module.QTimer, "singleShot", fake_single_shot)

    manager_with_profile._on_sync_event()

    assert delays.count(200) >= 2


def test_on_sync_event_skips_when_prioritysieve_enabled(manager_with_profile, monkeypatch):
    run_calls = {}

    def fake_run_after_sync(*args, **kwargs):
        run_calls["called"] = True

    manager_with_profile.run_after_sync = fake_run_after_sync  # type: ignore[assignment]
    manager_with_profile._prioritysieve_waiting_post_sync = False
    monkeypatch.setattr(manager_with_profile, "_prioritysieve_post_sync_active", lambda: True)

    manager_with_profile._on_sync_event()

    assert run_calls == {}
    assert manager_with_profile._prioritysieve_waiting_post_sync is True


def test_prioritysieve_post_sync_active_reads_config(manager_with_profile, monkeypatch):
    monkeypatch.setattr(manager_with_profile, "_prioritysieve_recalc_main", lambda: object())
    addon_manager = manager_with_profile.mw.addonManager

    def config_with_post_sync(module_name: str) -> dict:
        if module_name == "prioritysieve":
            return {"recalc_after_sync": True}
        return {}

    monkeypatch.setattr(addon_manager, "getConfig", config_with_post_sync)
    assert manager_with_profile._prioritysieve_post_sync_active() is True

    def config_without_post_sync(module_name: str) -> dict:
        if module_name == "prioritysieve":
            return {"recalc_after_sync": False}
        return {}

    monkeypatch.setattr(addon_manager, "getConfig", config_without_post_sync)
    assert manager_with_profile._prioritysieve_post_sync_active() is False


def test_handle_prioritysieve_recalc_completed_runs_when_pending(manager_with_profile):
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_run_after_sync(*args, **kwargs):
        calls.append((args, kwargs))

    manager_with_profile.run_after_sync = fake_run_after_sync  # type: ignore[assignment]
    manager_with_profile._prioritysieve_waiting_post_sync = True

    manager_with_profile._handle_prioritysieve_recalc_completed()

    assert len(calls) == 1
    assert manager_with_profile._prioritysieve_waiting_post_sync is False


def test_handle_prioritysieve_recalc_completed_noop_without_flag(manager_with_profile):
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_run_after_sync(*args, **kwargs):
        calls.append((args, kwargs))

    manager_with_profile.run_after_sync = fake_run_after_sync  # type: ignore[assignment]
    manager_with_profile._prioritysieve_waiting_post_sync = False

    manager_with_profile._handle_prioritysieve_recalc_completed()

    assert calls == []


def test_run_after_sync_without_followup(manager_with_profile, kanjicards_module, tmp_path):
    mw = FakeMainWindow(tmp_path)
    manager_with_profile.mw = mw
    cfg = manager_with_profile._config_from_raw(
        {
            "kanji_note_type": {"name": "Kanji", "fields": {}},
            "vocab_note_types": [],
            "auto_run_on_sync": True,
        }
    )
    manager_with_profile.load_config = lambda: cfg  # type: ignore[assignment]
    manager_with_profile._have_vocab_notes_changed = lambda collection, cfg: True  # type: ignore[assignment]
    manager_with_profile._stats_warrant_sync = lambda stats: True  # type: ignore[assignment]
    manager_with_profile.run_recalc = lambda: {"created": 1}  # type: ignore[assignment]

    called = {}

    def fake_trigger() -> bool:
        called["trigger"] = True
        return True

    manager_with_profile._trigger_followup_sync = fake_trigger  # type: ignore[assignment]

    results = []
    manager_with_profile.run_after_sync(
        allow_followup=False,
        on_finished=lambda changed: results.append(changed),
    )

    assert results == [True]
    assert called == {}
    assert manager_with_profile._suppress_next_auto_sync is False


def test_on_sync_event_runs_when_config_changed(manager_with_profile, kanjicards_module, tmp_path):
    mw = FakeMainWindow(tmp_path)
    manager_with_profile.mw = mw
    manager_with_profile._suppress_next_auto_sync = False
    raw_cfg = {
        "kanji_note_type": {
            "name": "Kanji",
            "fields": {
                "kanji": "Character",
                "definition": "Meaning",
                "stroke_count": "Strokes",
                "kunyomi": "Kun",
                "onyomi": "On",
                "frequency": "Freq",
            },
        },
        "vocab_note_types": [],
        "auto_run_on_sync": True,
    }
    cfg = manager_with_profile._config_from_raw(raw_cfg)
    manager_with_profile.load_config = lambda: cfg  # type: ignore[assignment]
    manager_with_profile._stats_warrant_sync = lambda stats: False  # type: ignore[assignment]
    manager_with_profile._have_vocab_notes_changed = lambda collection, cfg: False  # type: ignore[assignment]
    run_calls = []

    def fake_recalc_internal(**kwargs):
        manager_with_profile._pending_vocab_sync_marker = (0, 0)
        current_cfg = kwargs.get("cfg", cfg)
        manager_with_profile._pending_config_hash = manager_with_profile._hash_config(current_cfg)
        run_calls.append(True)
        return {"created": 0}

    manager_with_profile._recalc_internal = fake_recalc_internal  # type: ignore[assignment]
    manager_with_profile._trigger_followup_sync = lambda: False  # type: ignore[assignment]
    mw.col = object()
    manager_with_profile._last_synced_config_hash = "previous"
    expected_hash = manager_with_profile._hash_config(cfg)

    manager_with_profile._on_sync_event()

    assert run_calls
    assert manager_with_profile._last_synced_config_hash == expected_hash
    assert manager_with_profile._suppress_next_auto_sync is False


def test_on_sync_event_respects_suppression(manager_with_profile):
    manager_with_profile._suppress_next_auto_sync = True
    manager_with_profile._on_sync_event()
    assert manager_with_profile._suppress_next_auto_sync is False


def test_on_sync_event_skips_when_no_vocab_changes(manager_with_profile, tmp_path):
    mw = FakeMainWindow(tmp_path)
    manager_with_profile.mw = mw
    cfg = manager_with_profile._config_from_raw(
        {
            "kanji_note_type": {"name": "Kanji", "fields": {}},
            "vocab_note_types": [],
            "auto_run_on_sync": True,
        }
    )
    manager_with_profile.load_config = lambda: cfg  # type: ignore[assignment]
    manager_with_profile._have_vocab_notes_changed = lambda collection, cfg: False  # type: ignore[assignment]
    called = {}

    def fail_run_recalc():
        called["run"] = True
        return {}

    manager_with_profile.run_recalc = fail_run_recalc  # type: ignore[assignment]
    manager_with_profile._last_synced_config_hash = manager_with_profile._hash_config(cfg)
    mw.col = types.SimpleNamespace()
    manager_with_profile._on_sync_event()
    assert "run" not in called
