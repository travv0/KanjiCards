import importlib
import sys
import types


def test_import_fallbacks(monkeypatch):
    original_module = importlib.import_module("KanjiCards")
    original_utils = sys.modules["aqt.utils"]
    original_qt = sys.modules["aqt.qt"]

    # Build utils stub that only exposes legacy names to trigger the fallback import path.
    legacy_utils = types.ModuleType("aqt.utils")
    legacy_utils.showCritical = lambda *args, **kwargs: None
    legacy_utils.showInfo = lambda *args, **kwargs: None
    legacy_utils.showWarning = lambda *args, **kwargs: None
    legacy_utils.tooltip = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "aqt.utils", legacy_utils)

    # Create a Qt stub that lacks the new-style enum attributes.
    legacy_view = type(
        "LegacyView",
        (),
        {
            "SingleSelection": 11,
            "NoSelection": 0,
        },
    )
    legacy_qt = type(
        "LegacyQt",
        (),
        {
            "ItemIsUserCheckable": 0x10,
            "Checked": 2,
            "Unchecked": 0,
            "UserRole": 32,
        },
    )
    legacy_dialog = type(
        "LegacyDialog",
        (),
        {
            "Accepted": 1,
            "Rejected": 0,
        },
    )
    legacy_buttons = type(
        "LegacyButtons",
        (),
        {
            "Ok": 1,
            "Cancel": 0,
        },
    )

    qt_stub = types.ModuleType("aqt.qt")
    qt_stub.QAbstractItemView = legacy_view
    qt_stub.QDialog = legacy_dialog
    qt_stub.QDialogButtonBox = legacy_buttons
    qt_stub.Qt = legacy_qt

    # Reuse other Qt classes from the original stub so the import continues to work.
    for name in (
        "QCheckBox",
        "QComboBox",
        "QFormLayout",
        "QGroupBox",
        "QHBoxLayout",
        "QLabel",
        "QLineEdit",
        "QListWidget",
        "QListWidgetItem",
        "QPushButton",
        "QSpinBox",
        "QTabWidget",
        "QTimer",
        "QWidget",
        "QVBoxLayout",
        "QApplication",
    ):
        setattr(qt_stub, name, getattr(original_qt, name))

    monkeypatch.setitem(sys.modules, "aqt.qt", qt_stub)
    monkeypatch.delitem(sys.modules, "KanjiCards", raising=False)

    reloaded = importlib.import_module("KanjiCards")

    assert reloaded.show_critical is legacy_utils.showCritical
    assert reloaded.SINGLE_SELECTION == legacy_view.SingleSelection
    assert reloaded.NO_SELECTION == legacy_view.NoSelection
    assert reloaded.ITEM_IS_USER_CHECKABLE == legacy_qt.ItemIsUserCheckable
    assert reloaded.CHECKED_STATE == legacy_qt.Checked
    assert reloaded.UNCHECKED_STATE == legacy_qt.Unchecked
    assert reloaded.USER_ROLE == legacy_qt.UserRole
    assert reloaded.DIALOG_ACCEPTED == legacy_dialog.Accepted
    assert reloaded.DIALOG_REJECTED == legacy_dialog.Rejected
    assert reloaded.BUTTON_OK == legacy_buttons.Ok
    assert reloaded.BUTTON_CANCEL == legacy_buttons.Cancel

    # Restore the original module so subsequent tests continue to operate on the session-scoped instance.
    sys.modules["KanjiCards"] = original_module
    importlib.reload(original_module)
