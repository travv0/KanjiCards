import sys
import types
from pathlib import Path

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
        self.col = types.SimpleNamespace()

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
    assert "Recalculate Kanji Cards from Vocab" in labels
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

    calls: list[str] = []
    manager_with_profile.run_recalc = lambda: calls.append("kanjicards")  # type: ignore[assignment]

    manager_with_profile._on_toolbar_did_redraw(toolbar)
    handler = toolbar.link_handlers.get("kanjicards_recalc")
    assert callable(handler)
    handler()

    assert calls == ["kanjicards"]


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

    manager_with_profile.run_recalc = lambda: events.append("kanjicards")  # type: ignore[assignment]

    manager_with_profile._maybe_wrap_prioritysieve_recalc(fake_module)

    assert getattr(fake_module, "_kanjicards_recalc_wrapper_installed", False) is True
    assert manager_with_profile._prioritysieve_recalc_wrapped is True

    fake_module.recalc()

    assert events == ["priority_recalc", "priority_followup", "kanjicards"]


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
    manager_with_profile._recalc_internal = lambda **kwargs: {"created": 1}  # type: ignore[assignment]
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

    manager_with_profile._recalc_internal = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[assignment]
    called = {}
    monkeypatch.setattr(kanjicards_module, "show_critical", lambda message: called.setdefault("message", message))
    assert manager_with_profile.run_recalc() is None
    assert "boom" in called["message"]


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
    monkeypatch.setattr(manager_with_profile, "_prioritysieve_post_sync_active", lambda: True)

    manager_with_profile._on_sync_event()

    assert run_calls == {}


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
