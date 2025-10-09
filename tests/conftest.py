import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
import types
from pathlib import Path
from typing import Any, Dict

import pytest


class _Hook:
    def __init__(self) -> None:
        self.callbacks = []

    def append(self, callback) -> None:
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def remove(self, callback) -> None:
        try:
            self.callbacks.remove(callback)
        except ValueError:
            pass

    def __call__(self, *args, **kwargs) -> None:
        for callback in list(self.callbacks):
            callback(*args, **kwargs)


class _Signal:
    def __init__(self) -> None:
        self._receivers = []

    def connect(self, receiver) -> None:
        if receiver not in self._receivers:
            self._receivers.append(receiver)

    def emit(self, *args, **kwargs) -> None:
        for receiver in list(self._receivers):
            receiver(*args, **kwargs)


def _make_qt_module() -> types.ModuleType:
    qt_mod = types.ModuleType("aqt.qt")

    class QApplication:
        @staticmethod
        def processEvents() -> None:
            return None

    class QDialog:
        class DialogCode:
            Accepted = 1
            Rejected = 0

        Accepted = 1
        Rejected = 0

        def __init__(self, *args, **kwargs) -> None:
            return None

        def exec(self) -> int:
            return self.Accepted

    class QAbstractItemView:
        class SelectionMode:
            SingleSelection = 1
            NoSelection = 0

        SingleSelection = 1
        NoSelection = 0

    class Qt:
        class ItemFlag:
            ItemIsUserCheckable = 1

        class CheckState:
            Checked = 2
            Unchecked = 0

        class ItemDataRole:
            UserRole = 32

    class QDialogButtonBox:
        class StandardButton:
            Ok = 1
            Cancel = 0

        Ok = 1
        Cancel = 0

        def __init__(self, *args, **kwargs) -> None:
            self.accepted = _Signal()
            self.rejected = _Signal()

        def connect(self, *args, **kwargs) -> None:
            return None

    def _widget_factory(name: str):
        return type(
            name,
            (),
            {
                "__init__": lambda self, *args, **kwargs: None,
            },
        )

    class QListWidget:
        def __init__(self, *args, **kwargs) -> None:
            self._items = []

        def setSelectionMode(self, *args, **kwargs) -> None:
            return None

        def addItem(self, item) -> None:
            self._items.append(item)

        def clear(self) -> None:
            self._items.clear()

        def count(self) -> int:
            return len(self._items)

        def item(self, index: int):
            return self._items[index]

    class QListWidgetItem:
        def __init__(self, text="") -> None:
            self._text = text
            self._flags = 0
            self._check_state = 0

        def text(self) -> str:
            return self._text

        def setFlags(self, flags) -> None:
            self._flags = flags

        def flags(self):
            return self._flags

        def setCheckState(self, state) -> None:
            self._check_state = state

        def checkState(self):
            return self._check_state

    class QComboBox:
        def __init__(self, *args, **kwargs) -> None:
            self._items = []
            self._index = 0
            self.currentIndexChanged = _Signal()

        def addItem(self, item: str) -> None:
            self._items.append(item)

        def currentIndex(self) -> int:
            return self._index

        def setCurrentIndex(self, index: int) -> None:
            if 0 <= index < len(self._items):
                self._index = index
                self.currentIndexChanged.emit(index)

    class QSpinBox:
        def __init__(self, *args, **kwargs) -> None:
            self._value = 0
            self._minimum = 0
            self._maximum = 0

        def setRange(self, minimum: int, maximum: int) -> None:
            self._minimum = minimum
            self._maximum = maximum

        def setValue(self, value: int) -> None:
            self._value = value

        def value(self) -> int:
            return self._value

        def setSingleStep(self, _step: int) -> None:
            return None

        def setSuffix(self, _suffix: str) -> None:
            return None

    class QTimer:
        @staticmethod
        def singleShot(*args, **kwargs) -> None:
            return None

    qt_mod.QAbstractItemView = QAbstractItemView
    qt_mod.QCheckBox = _widget_factory("QCheckBox")
    qt_mod.QComboBox = QComboBox
    qt_mod.QDialog = QDialog
    qt_mod.QDialogButtonBox = QDialogButtonBox
    qt_mod.QFormLayout = _widget_factory("QFormLayout")
    qt_mod.QGroupBox = _widget_factory("QGroupBox")
    qt_mod.QHBoxLayout = _widget_factory("QHBoxLayout")
    qt_mod.QLabel = _widget_factory("QLabel")
    qt_mod.QLineEdit = _widget_factory("QLineEdit")
    qt_mod.QListWidget = QListWidget
    qt_mod.QListWidgetItem = QListWidgetItem
    qt_mod.QPushButton = _widget_factory("QPushButton")
    qt_mod.QSpinBox = QSpinBox
    qt_mod.QTabWidget = _widget_factory("QTabWidget")
    qt_mod.Qt = Qt
    qt_mod.QTimer = QTimer
    qt_mod.QWidget = _widget_factory("QWidget")
    qt_mod.QVBoxLayout = _widget_factory("QVBoxLayout")
    qt_mod.QApplication = QApplication
    return qt_mod


def _install_stubs() -> None:
    if "anki" in sys.modules:
        return

    anki_pkg = types.ModuleType("anki")
    anki_pkg.__path__ = ["<anki>"]  # type: ignore[attr-defined]
    collection_mod = types.ModuleType("anki.collection")

    class Collection:
        pass

    collection_mod.Collection = Collection

    models_mod = types.ModuleType("anki.models")
    models_mod.NotetypeDict = Dict[str, Any]

    notes_mod = types.ModuleType("anki.notes")

    class Note:
        pass

    notes_mod.Note = Note

    utils_mod = types.ModuleType("anki.utils")

    def intTime() -> int:
        return 0

    utils_mod.intTime = intTime

    sys.modules["anki"] = anki_pkg
    sys.modules["anki.collection"] = collection_mod
    sys.modules["anki.models"] = models_mod
    sys.modules["anki.notes"] = notes_mod
    sys.modules["anki.utils"] = utils_mod

    gui_hooks = types.SimpleNamespace(
        profile_did_open=_Hook(),
        main_window_did_init=_Hook(),
        reviewer_did_answer_card=_Hook(),
        reviewer_did_show_question=_Hook(),
        sync_did_finish=_Hook(),
        sync_will_start=_Hook(),
    )

    utils_module = types.ModuleType("aqt.utils")

    def _noop(*args, **kwargs):
        return None

    utils_module.show_critical = _noop
    utils_module.show_info = _noop
    utils_module.show_warning = _noop
    utils_module.tooltip = _noop

    aqt_pkg = types.ModuleType("aqt")
    aqt_pkg.__path__ = ["<aqt>"]  # type: ignore[attr-defined]
    aqt_pkg.gui_hooks = gui_hooks
    aqt_pkg.mw = None

    qt_mod = _make_qt_module()

    sys.modules["aqt"] = aqt_pkg
    sys.modules["aqt.gui_hooks"] = gui_hooks  # type: ignore[assignment]
    sys.modules["aqt.utils"] = utils_module
    sys.modules["aqt.qt"] = qt_mod

    class _AddonFinder(importlib.abc.MetaPathFinder):
        def __init__(self, module_name: str, module_path: Path) -> None:
            self._module_name = module_name
            self._module_path = module_path

        def find_spec(self, fullname, path=None, target=None):
            if fullname != self._module_name:
                return None
            loader = importlib.machinery.SourceFileLoader(fullname, str(self._module_path))
            return importlib.util.spec_from_loader(fullname, loader)

    addon_path = Path(__file__).resolve().parent.parent / "__init__.py"
    finder = _AddonFinder("KanjiCards", addon_path)
    if not any(isinstance(existing, _AddonFinder) for existing in sys.meta_path):
        sys.meta_path.insert(0, finder)

_install_stubs()


@pytest.fixture(scope="session", autouse=True)
def stub_anki_env() -> None:
    # Stubs are installed at import time; fixture ensures pytest keeps them for session.
    yield


@pytest.fixture(scope="session")
def kanjicards_module(stub_anki_env):
    try:
        return importlib.import_module("KanjiCards")
    except ModuleNotFoundError:
        module_path = Path(__file__).resolve().parent.parent / "__init__.py"
        spec = importlib.util.spec_from_file_location("KanjiCards", module_path)
        if spec is None or spec.loader is None:
            raise
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("KanjiCards", module)
        spec.loader.exec_module(module)
        return module


@pytest.fixture
def manager_with_profile(kanjicards_module, tmp_path):
    class _AddonManager:
        def __init__(self, folder: str) -> None:
            self._folder = folder
            self.config_actions = {}
            self.written_configs = []
            self._config = {}

        def addonFromModule(self, module_name: str) -> str:
            return "KanjiCards"

        def addonsFolder(self) -> str:
            return self._folder

        def setConfigAction(self, module_name: str, action) -> None:
            self.config_actions[module_name] = action

        def getConfig(self, module_name: str) -> dict:
            return dict(self._config)

        def writeConfig(self, module_name: str, data: dict) -> None:
            self._config = dict(data)
            self.written_configs.append((module_name, data))

    manager = kanjicards_module.KanjiVocabSyncManager.__new__(kanjicards_module.KanjiVocabSyncManager)
    profile_dir = tmp_path / "profile"
    profile_dir.mkdir()
    addons_folder = tmp_path / "addons"
    addons_folder.mkdir()
    manager.mw = types.SimpleNamespace(
        pm=types.SimpleNamespace(profileFolder=lambda: str(profile_dir)),
        addonManager=_AddonManager(str(addons_folder)),
    )
    manager.addon_name = "KanjiCards"
    manager.addon_dir = str(tmp_path / "addon")
    os.makedirs(manager.addon_dir, exist_ok=True)
    manager._debug_path = str(tmp_path / "debug.log")
    manager._debug_enabled = False
    manager._profile_config_error_logged = False
    manager._dictionary_cache = None
    manager._existing_notes_cache = None
    manager._kanji_model_cache = None
    manager._vocab_model_cache = None
    manager._realtime_error_logged = False
    manager._missing_deck_logged = False
    manager._sync_hook_installed = False
    manager._sync_hook_target = None
    manager._last_vocab_sync_mod = None
    manager._last_vocab_sync_count = None
    manager._pending_vocab_sync_marker = None
    manager._suppress_next_auto_sync = False
    return manager
