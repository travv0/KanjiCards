"""KanjiCards add-on.

This add-on inspects configured vocabulary notes that the user has reviewed
and ensures each kanji found in those notes has a corresponding kanji card.
Existing kanji cards receive a configurable tag, and missing ones are created
automatically using dictionary data and tagged accordingly.
"""
from __future__ import annotations

import builtins
import json
import hashlib
import os
import re
import sys
import time
from collections import defaultdict
import xml.etree.ElementTree as ET
from functools import wraps
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
from types import ModuleType

from anki.collection import Collection
from anki.models import NotetypeDict
from anki.notes import Note
from anki.utils import intTime
from aqt import gui_hooks, mw
try:
    from aqt.toolbar import Toolbar
except Exception:  # pragma: no cover - toolbar not available in some test environments
    Toolbar = Any  # type: ignore[assignment]
from aqt.qt import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSpinBox,
    QTabWidget,
    Qt,
    QTimer,
    QWidget,
    QVBoxLayout,
)
from aqt.qt import QApplication

# Messaging helpers differ between Qt versions, so prefer new-style names.
try:
    from aqt.utils import show_critical, show_info, show_warning, tooltip
except ImportError:
    from aqt.utils import showCritical as show_critical
    from aqt.utils import showInfo as show_info
    from aqt.utils import showWarning as show_warning
    from aqt.utils import tooltip

try:
    from aqt.utils import askUser
except ImportError:
    askUser = None  # type: ignore[assignment]

try:
    from aqt.qt import QMessageBox
except ImportError:
    QMessageBox = None  # type: ignore[assignment]

try:  # PyQt6-style enums
    SINGLE_SELECTION = QAbstractItemView.SelectionMode.SingleSelection
    NO_SELECTION = QAbstractItemView.SelectionMode.NoSelection
except AttributeError:
    SINGLE_SELECTION = QAbstractItemView.SingleSelection
    NO_SELECTION = QAbstractItemView.NoSelection

try:
    ITEM_IS_USER_CHECKABLE = Qt.ItemFlag.ItemIsUserCheckable
    CHECKED_STATE = Qt.CheckState.Checked
    UNCHECKED_STATE = Qt.CheckState.Unchecked
    USER_ROLE = Qt.ItemDataRole.UserRole
except AttributeError:
    ITEM_IS_USER_CHECKABLE = Qt.ItemIsUserCheckable
    CHECKED_STATE = Qt.Checked
    UNCHECKED_STATE = Qt.Unchecked
    USER_ROLE = Qt.UserRole

try:
    DIALOG_ACCEPTED = QDialog.DialogCode.Accepted
    DIALOG_REJECTED = QDialog.DialogCode.Rejected
except AttributeError:
    DIALOG_ACCEPTED = QDialog.Accepted
    DIALOG_REJECTED = QDialog.Rejected

try:
    BUTTON_OK = QDialogButtonBox.StandardButton.Ok
    BUTTON_CANCEL = QDialogButtonBox.StandardButton.Cancel
except AttributeError:
    BUTTON_OK = QDialogButtonBox.Ok
    BUTTON_CANCEL = QDialogButtonBox.Cancel

KANJI_PATTERN = re.compile(r"[\u3400-\u9FFF\uF900-\uFAFF]")

SQLITE_MAX_VARIABLES = 900

BUCKET_TAG_KEYS: Tuple[str, str, str] = (
    "reviewed_vocab",
    "unreviewed_vocab",
    "no_vocab",
)

SCHEDULING_FIELD_DEFAULT_NAME = "KanjiCards Scheduling Info"

KANJICARDS_TOOLBAR_CMD = "kanjicards_recalc"
KANJICARDS_TOOLBAR_ID = "kanjicards_recalc_toolbar"
PRIORITYSIEVE_TOOLBAR_CMD = "recalc_toolbar"


def _safe_print(*args: object, **kwargs: Any) -> None:
    try:
        builtins.print(*args, **kwargs)
        return
    except (BlockingIOError, OSError):
        pass
    except Exception:
        return

    try:
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        stream = kwargs.get("file")
    except Exception:
        sep = " "
        end = "\n"
        stream = None

    try:
        message = sep.join(str(arg) for arg in args)
    except Exception:
        return

    fallback_stream = stream or getattr(sys, "__stderr__", None) or getattr(sys, "__stdout__", None)
    if fallback_stream is None:
        return
    try:
        fallback_stream.write(message + end)
    except Exception:
        pass


@dataclass
class VocabNoteTypeConfig:
    name: str
    fields: List[str] = field(default_factory=list)
    due_multiplier: float = 1.0


@dataclass
class KanjiNoteTypeConfig:
    name: str
    fields: Dict[str, str] = field(default_factory=dict)


@dataclass
class AddonConfig:
    vocab_note_types: List[VocabNoteTypeConfig]
    kanji_note_type: KanjiNoteTypeConfig
    existing_tag: str
    created_tag: str
    bucket_tags: Dict[str, str]
    only_new_vocab_tag: str
    no_vocab_tag: str
    dictionary_file: str
    kanji_deck_name: str
    auto_run_on_sync: bool
    realtime_review: bool
    unsuspended_tag: str
    reorder_mode: str
    ignore_suspended_vocab: bool
    known_kanji_interval: int
    auto_suspend_vocab: bool
    auto_suspend_tag: str
    resuspend_reviewed_low_interval: bool
    low_interval_vocab_tag: str
    store_scheduling_info: bool


@dataclass
class KanjiUsageInfo:
    reviewed: bool = False
    first_review_order: Optional[int] = None
    first_review_due: Optional[int] = None
    first_new_due: Optional[int] = None
    first_new_order: Optional[int] = None
    vocab_occurrences: int = 0


@dataclass
class KanjiIntervalStatus:
    has_review_card: bool = False
    current_interval: int = 0
    historical_interval: int = 0

    @property
    def has_history(self) -> bool:
        return self.historical_interval > 0 or self.has_review_card


class KanjiVocabRecalcManager:
    """Core coordinator for the KanjiCards add-on."""

    def __init__(self) -> None:
        if not mw:
            raise RuntimeError("KanjiCards requires Anki main window")
        self.mw = mw
        self._dictionary_cache: Optional[Dict[str, Any]] = None
        self._existing_notes_cache: Optional[Dict[str, Any]] = None
        self._kanji_model_cache: Optional[Dict[str, Any]] = None
        self._vocab_model_cache: Optional[Dict[str, Any]] = None
        self._realtime_error_logged = False
        self._missing_deck_logged = False
        self._sync_hook_installed = False
        self._sync_hook_target: Optional[str] = None
        self._profile_config_error_logged = False
        self._profile_state_error_logged = False
        self._pre_answer_card_state: Dict[int, Dict[str, Optional[int]]] = {}
        self._last_question_card_id: Optional[int] = None
        self._debug_path: Optional[str] = None
        self._debug_enabled = False
        self._last_vocab_sync_mod: Optional[int] = None
        self._last_vocab_sync_count: Optional[int] = None
        self._pending_vocab_sync_marker: Optional[Tuple[int, int]] = None
        self._last_synced_config_hash: Optional[str] = None
        self._pending_config_hash: Optional[str] = None
        self._recalc_action = None
        self._prioritysieve_recalc_wrapped = False
        self._prioritysieve_waiting_post_sync = False
        self._prioritysieve_toolbar_followup = False
        self.addon_name = self.mw.addonManager.addonFromModule(__name__)
        if self.addon_name:
            self.addon_dir = os.path.join(self.mw.addonManager.addonsFolder(), self.addon_name)
        else:
            # Fallback for development environments where the addon manager does not know the module.
            self.addon_dir = os.path.dirname(__file__)
        self._debug_path = os.path.join(self.addon_dir, "kanjicards_debug.log")
        self._debug("manager_init", addon_dir=self.addon_dir)
        self._ensure_menu_actions()
        self._install_hooks()
        self._install_sync_hook()
        self.mw.addonManager.setConfigAction(__name__, self.show_settings)
        self._suppress_next_auto_sync = False

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def _profile_config_path(self) -> Optional[str]:
        pm = getattr(self.mw, "pm", None)
        if pm is None:
            return None
        try:
            folder = pm.profileFolder()
        except Exception:
            return None
        if not folder:
            return None
        return os.path.join(folder, "kanjicards_config.json")

    def _profile_state_path(self) -> Optional[str]:
        pm = getattr(self.mw, "pm", None)
        if pm is None:
            return None
        try:
            folder = pm.profileFolder()
        except Exception:
            return None
        if not folder:
            return None
        return os.path.join(folder, "kanjicards_state.json")

    def _debug(self, message: str, **extra: object) -> None:
        if not self._debug_enabled:
            return
        path = self._debug_path
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            payload = message
            if extra:
                try:
                    serialized = json.dumps(extra, ensure_ascii=False, sort_keys=True, default=str)
                except Exception:
                    serialized = str(extra)
                payload = f"{payload} {serialized}"
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"{timestamp} {payload}\n")
        except Exception:
            pass

    def _apply_profile_state_payload(self, payload: Dict[str, Any]) -> None:
        def _coerce_int(value: Any) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(value)
            except Exception:
                return None

        self._last_vocab_sync_mod = _coerce_int(payload.get("last_vocab_sync_mod"))
        self._last_vocab_sync_count = _coerce_int(payload.get("last_vocab_sync_count"))
        config_hash = payload.get("last_config_hash")
        if isinstance(config_hash, str) and config_hash:
            self._last_synced_config_hash = config_hash
        else:
            self._last_synced_config_hash = None

    def _extract_legacy_profile_state(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        legacy_state: Dict[str, Any] = {}
        removed = False
        for key in ("last_vocab_sync_mod", "last_vocab_sync_count", "last_config_hash"):
            if key in data:
                legacy_state[key] = data.pop(key)
                removed = True
        return legacy_state, removed

    def _load_profile_state(self) -> None:
        path = self._profile_state_path()
        if not path or not os.path.exists(path):
            self._profile_state_error_logged = False
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception as err:  # noqa: BLE001
            if not self._profile_state_error_logged:
                _safe_print(f"[KanjiCards] Failed to load profile state: {err}")
                self._profile_state_error_logged = True
            return
        self._profile_state_error_logged = False
        if isinstance(payload, dict):
            self._apply_profile_state_payload(payload)

    def _write_profile_state(self) -> None:
        path = self._profile_state_path()
        if not path:
            return
        state_payload: Dict[str, Any] = {}
        if self._last_vocab_sync_mod is not None:
            state_payload["last_vocab_sync_mod"] = int(self._last_vocab_sync_mod)
        if self._last_vocab_sync_count is not None:
            state_payload["last_vocab_sync_count"] = int(self._last_vocab_sync_count)
        if self._last_synced_config_hash:
            state_payload["last_config_hash"] = self._last_synced_config_hash
        if not state_payload:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except Exception as err:  # noqa: BLE001
                if not self._profile_state_error_logged:
                    _safe_print(f"[KanjiCards] Failed to remove profile state: {err}")
                    self._profile_state_error_logged = True
            else:
                self._profile_state_error_logged = False
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(state_payload, handle, indent=2, ensure_ascii=False)
        except Exception as err:  # noqa: BLE001
            if not self._profile_state_error_logged:
                _safe_print(f"[KanjiCards] Failed to write profile state: {err}")
                self._profile_state_error_logged = True
        else:
            self._profile_state_error_logged = False

    def _load_profile_config(self) -> Dict[str, Any]:
        path = self._profile_config_path()
        config_data: Dict[str, Any] = {}
        legacy_state: Dict[str, Any] = {}
        legacy_removed = False
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    raw = json.load(handle)
            except Exception as err:  # noqa: BLE001
                if not self._profile_config_error_logged:
                    _safe_print(f"[KanjiCards] Failed to load profile config: {err}")
                    self._profile_config_error_logged = True
                self._apply_profile_state_payload({})
                self._load_profile_state()
                return {}
            self._profile_config_error_logged = False
            if isinstance(raw, dict):
                config_data = dict(raw)
                legacy_state, legacy_removed = self._extract_legacy_profile_state(config_data)
            else:
                self._apply_profile_state_payload({})
                self._load_profile_state()
                return {}
        else:
            self._profile_config_error_logged = False
        if legacy_state:
            self._apply_profile_state_payload(legacy_state)
        else:
            self._apply_profile_state_payload({})
        self._load_profile_state()
        if legacy_removed:
            self._write_profile_config(config_data)
        return config_data

    def _load_profile_config_or_seed(self, global_cfg: Dict[str, Any]) -> Dict[str, Any]:
        path = self._profile_config_path()
        if not path:
            return {}
        if not os.path.exists(path):
            base_raw = global_cfg if isinstance(global_cfg, dict) else {}
            seed_cfg = self._config_from_raw(base_raw)
            self._write_profile_config(self._serialize_config(seed_cfg))
        return self._load_profile_config()

    def _write_profile_config(self, data: Dict[str, Any]) -> None:
        path = self._profile_config_path()
        if not path:
            return
        write_succeeded = False
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = dict(data)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)
            write_succeeded = True
        except Exception as err:  # noqa: BLE001
            _safe_print(f"[KanjiCards] Failed to write profile config: {err}")
        if write_succeeded:
            self._write_profile_state()

    def _merge_config_sources(self, global_cfg: Dict[str, Any], profile_cfg: Dict[str, Any]) -> Dict[str, Any]:
        if not profile_cfg:
            return dict(global_cfg)

        merged = dict(global_cfg)
        for key, value in profile_cfg.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_config_sources(merged[key], value)  # type: ignore[arg-type]
            else:
                merged[key] = value
        return merged

    def _normalize_kanji_fields(self, raw_fields: Optional[Dict[str, object]]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if isinstance(raw_fields, dict):
            for key, value in raw_fields.items():
                if isinstance(value, str):
                    result[key] = value
                elif value is None:
                    result[key] = ""
                else:
                    result[key] = str(value)
        for logical_name in ("kanji", "definition", "stroke_count", "kunyomi", "onyomi", "frequency", "scheduling_info"):
            result.setdefault(logical_name, "")
        return result

    def _normalize_bucket_tags(self, raw_tags: Optional[Dict[str, object]]) -> Dict[str, str]:
        result: Dict[str, str] = {key: "" for key in BUCKET_TAG_KEYS}
        if not isinstance(raw_tags, dict):
            return result
        for key in BUCKET_TAG_KEYS:
            value = raw_tags.get(key, "")
            if isinstance(value, str):
                result[key] = value.strip()
            elif value is None:
                result[key] = ""
            else:
                result[key] = str(value).strip()
        return result

    def _config_from_raw(self, raw: Dict[str, Any]) -> AddonConfig:
        vocab_entries = raw.get("vocab_note_types", [])
        vocab_cfg: List[VocabNoteTypeConfig] = []
        if isinstance(vocab_entries, list):
            for item in vocab_entries:
                if not isinstance(item, dict):
                    continue
                fields_raw = item.get("fields", []) or []
                fields = [field for field in fields_raw if isinstance(field, str)]
                multiplier_raw = item.get("due_multiplier", 1.0)
                try:
                    multiplier_val = float(multiplier_raw)
                except Exception:
                    multiplier_val = 1.0
                if multiplier_val <= 0:
                    multiplier_val = 1.0
                vocab_cfg.append(
                    VocabNoteTypeConfig(
                        name=item.get("note_type", ""),
                        fields=fields,
                        due_multiplier=multiplier_val,
                    )
                )

        kanji_cfg_raw = raw.get("kanji_note_type", {})
        if not isinstance(kanji_cfg_raw, dict):
            kanji_cfg_raw = {}
        kanji_fields = self._normalize_kanji_fields(kanji_cfg_raw.get("fields"))
        kanji_cfg = KanjiNoteTypeConfig(
            name=kanji_cfg_raw.get("name", ""),
            fields=kanji_fields,
        )

        bucket_tags = self._normalize_bucket_tags(raw.get("bucket_tags"))

        interval_raw = raw.get("known_kanji_interval", 21)
        try:
            interval_value = int(interval_raw)
        except Exception:
            interval_value = 21
        if interval_value < 0:
            interval_value = 0
        return AddonConfig(
            vocab_note_types=vocab_cfg,
            kanji_note_type=kanji_cfg,
            existing_tag=raw.get("existing_tag", "has_vocab_kanji"),
            created_tag=raw.get("created_tag", "auto_kanji_card"),
            bucket_tags=bucket_tags,
            only_new_vocab_tag=raw.get("only_new_vocab_tag", ""),
            no_vocab_tag=raw.get("no_vocab_tag", ""),
            dictionary_file=raw.get("dictionary_file", "kanjidic2.xml"),
            kanji_deck_name=raw.get("kanji_deck_name", ""),
            auto_run_on_sync=bool(raw.get("auto_run_on_sync", False)),
            realtime_review=bool(raw.get("realtime_review", True)),
            unsuspended_tag=raw.get("unsuspended_tag", "kanjicards_unsuspended"),
            reorder_mode=raw.get("reorder_mode", "vocab"),
            ignore_suspended_vocab=bool(raw.get("ignore_suspended_vocab", False)),
            known_kanji_interval=interval_value,
            auto_suspend_vocab=bool(raw.get("auto_suspend_vocab", False)),
            auto_suspend_tag=raw.get("auto_suspend_tag", "kanjicards_new"),
            resuspend_reviewed_low_interval=bool(raw.get("resuspend_reviewed_low_interval", False)),
            low_interval_vocab_tag=raw.get("low_interval_vocab_tag", ""),
            store_scheduling_info=bool(raw.get("store_scheduling_info", False)),
        )

    def _serialize_config(self, cfg: AddonConfig) -> Dict[str, Any]:
        return {
            "vocab_note_types": [
                {
                    "note_type": item.name,
                    "fields": list(item.fields),
                    "due_multiplier": float(item.due_multiplier),
                }
                for item in cfg.vocab_note_types
            ],
            "kanji_note_type": {
                "name": cfg.kanji_note_type.name,
                "fields": dict(cfg.kanji_note_type.fields),
            },
            "existing_tag": cfg.existing_tag,
            "created_tag": cfg.created_tag,
            "bucket_tags": dict(cfg.bucket_tags),
            "only_new_vocab_tag": cfg.only_new_vocab_tag,
            "no_vocab_tag": cfg.no_vocab_tag,
            "dictionary_file": cfg.dictionary_file,
            "kanji_deck_name": cfg.kanji_deck_name,
            "auto_run_on_sync": bool(cfg.auto_run_on_sync),
            "realtime_review": bool(cfg.realtime_review),
            "unsuspended_tag": cfg.unsuspended_tag,
            "reorder_mode": cfg.reorder_mode,
            "ignore_suspended_vocab": bool(cfg.ignore_suspended_vocab),
            "known_kanji_interval": int(cfg.known_kanji_interval),
            "auto_suspend_vocab": bool(cfg.auto_suspend_vocab),
            "auto_suspend_tag": cfg.auto_suspend_tag,
            "resuspend_reviewed_low_interval": bool(cfg.resuspend_reviewed_low_interval),
            "low_interval_vocab_tag": cfg.low_interval_vocab_tag,
            "store_scheduling_info": bool(cfg.store_scheduling_info),
        }

    def _hash_config(self, cfg: AddonConfig) -> str:
        serialized = self._serialize_config(cfg)
        payload = json.dumps(serialized, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def load_config(self) -> AddonConfig:
        global_raw_obj = self.mw.addonManager.getConfig(__name__)
        global_raw = global_raw_obj if isinstance(global_raw_obj, dict) else {}
        profile_raw = self._load_profile_config_or_seed(global_raw)
        raw = self._merge_config_sources(global_raw, profile_raw)
        return self._config_from_raw(raw)

    def save_config(self, cfg: AddonConfig) -> None:
        raw = self._serialize_config(cfg)
        self.mw.addonManager.writeConfig(__name__, raw)
        self._write_profile_config(raw)
        self._dictionary_cache = None
        self._existing_notes_cache = None
        self._kanji_model_cache = None
        self._vocab_model_cache = None
        self._realtime_error_logged = False
        self._missing_deck_logged = False
        self._sync_hook_installed = False
        self._sync_hook_target = None
        self._install_sync_hook()

    # ------------------------------------------------------------------
    # UI wiring
    # ------------------------------------------------------------------
    def _ensure_menu_actions(self) -> None:
        menu = self.mw.form.menuTools
        recalc_action = menu.addAction("Recalculate Kanji Cards from Vocab")
        recalc_action.triggered.connect(self.run_recalc)
        self._recalc_action = recalc_action

        settings_action = menu.addAction("KanjiCards Settings")
        settings_action.triggered.connect(self.show_settings)
        self._settings_action = settings_action

    def _install_hooks(self) -> None:
        try:
            gui_hooks.reviewer_did_answer_card.remove(self._on_reviewer_did_answer_card)
        except (ValueError, AttributeError):
            pass
        try:
            gui_hooks.reviewer_did_show_question.remove(self._on_reviewer_did_show_question)
        except (ValueError, AttributeError):
            pass
        gui_hooks.reviewer_did_show_question.append(self._on_reviewer_did_show_question)
        gui_hooks.reviewer_did_answer_card.append(self._on_reviewer_did_answer_card)
        toolbar_links_hook = getattr(gui_hooks, "top_toolbar_did_init_links", None)
        if toolbar_links_hook is not None:
            try:
                toolbar_links_hook.remove(self._on_top_toolbar_init_links)
            except (ValueError, AttributeError):
                pass
            toolbar_links_hook.append(self._on_top_toolbar_init_links)
        toolbar_redraw_hook = getattr(gui_hooks, "toolbar_did_redraw", None)
        if toolbar_redraw_hook is not None:
            try:
                toolbar_redraw_hook.remove(self._on_toolbar_did_redraw)
            except (ValueError, AttributeError):
                pass
            toolbar_redraw_hook.append(self._on_toolbar_did_redraw)
        self._install_sync_hook()
        self._maybe_wrap_prioritysieve_recalc()

    def _install_sync_hook(self) -> None:
        if self._sync_hook_installed:
            return
        for hook_name in ("sync_did_finish", "sync_will_start"):
            hook = getattr(gui_hooks, hook_name, None)
            if hook is None:
                continue
            try:
                hook.remove(self._on_sync_event)
            except (ValueError, AttributeError):
                pass
            hook.append(self._on_sync_event)
            self._sync_hook_installed = True
            self._sync_hook_target = hook_name
            break

    def show_settings(self) -> None:
        dialog = KanjiVocabRecalcSettingsDialog(self, self.load_config())
        dialog.exec()

    def _prioritysieve_recalc_main(self) -> Optional[ModuleType]:
        module = sys.modules.get("prioritysieve.recalc.recalc_main")
        if module is None:
            try:
                module = __import__(
                    "prioritysieve.recalc.recalc_main",
                    fromlist=["recalc", "set_followup_sync_callback"],
                )
            except Exception:
                return None
        if not hasattr(module, "recalc") or not hasattr(module, "set_followup_sync_callback"):
            return None
        return module  # type: ignore[return-value]

    def _maybe_wrap_prioritysieve_recalc(self, ps_main: Optional[ModuleType] = None) -> None:
        if not hasattr(self, "_prioritysieve_recalc_wrapped"):
            setattr(self, "_prioritysieve_recalc_wrapped", False)
        if self._prioritysieve_recalc_wrapped:
            return
        if ps_main is None:
            ps_main = self._prioritysieve_recalc_main()
        if ps_main is None:
            return
        if getattr(ps_main, "_kanjicards_recalc_wrapper_installed", False):
            self._prioritysieve_recalc_wrapped = True
            return
        original_recalc = getattr(ps_main, "recalc", None)
        if not callable(original_recalc):
            return
        self._ensure_prioritysieve_completion_hooks(ps_main)

        manager = self

        @wraps(original_recalc)
        def wrapped_recalc(*args: object, **kwargs: object) -> object:
            previous_callback = getattr(ps_main, "_followup_sync_callback", None)
            completion_state: Dict[str, bool] = {"done": False}

            def _mark_completed() -> None:
                if completion_state.get("done"):
                    return
                completion_state["done"] = True
                manager._run_on_main(manager._handle_prioritysieve_recalc_completed)

            def _call_original() -> object:
                try:
                    return original_recalc(*args, **kwargs)
                except TypeError as err:
                    if args or kwargs:
                        try:
                            return original_recalc()
                        except TypeError:
                            raise err
                    raise

            def _after_prioritysieve_recalc() -> None:
                def _finish() -> None:
                    try:
                        if callable(previous_callback):
                            previous_callback()
                    except Exception:
                        pass
                    finally:
                        _mark_completed()

                manager._run_on_main(_finish)

            try:
                ps_main.set_followup_sync_callback(_after_prioritysieve_recalc)
            except Exception:
                result = _call_original()
                if callable(previous_callback):
                    manager._run_on_main(previous_callback)
                _mark_completed()
                return result
            try:
                result = _call_original()
            except Exception:
                try:
                    ps_main.set_followup_sync_callback(previous_callback)
                except Exception:
                    pass
                _mark_completed()
                raise
            if not completion_state.get("done"):
                manager._schedule_prioritysieve_completion_check(ps_main, completion_state)
            return result

        setattr(ps_main, "recalc", wrapped_recalc)
        setattr(ps_main, "_kanjicards_recalc_wrapper_installed", True)
        self._prioritysieve_recalc_wrapped = True

    def _run_on_main(self, callback: Callable[[], None]) -> None:
        try:
            taskman = getattr(self.mw, "taskman", None)
            if taskman and hasattr(taskman, "run_on_main"):
                try:
                    taskman.run_on_main(callback)
                    return
                except Exception:
                    pass
            callback()
        except Exception:
            pass

    def _call_later(self, callback: Callable[[], None], delay_ms: int = 0) -> None:
        if delay_ms <= 0:
            self._run_on_main(callback)
            return
        try:
            QTimer.singleShot(delay_ms, callback)
            return
        except Exception:
            pass
        try:
            import threading

            timer = threading.Timer(delay_ms / 1000.0, lambda: self._run_on_main(callback))
            timer.daemon = True
            timer.start()
        except Exception:
            pass

    def _schedule_prioritysieve_completion_check(
        self,
        ps_main: ModuleType,
        completion_state: Dict[str, bool],
        *,
        delay_ms: int = 200,
    ) -> None:
        if completion_state.get("done"):
            return

        def _check() -> None:
            if completion_state.get("done"):
                return
            recalc_in_progress = getattr(ps_main, "recalc_in_progress", None)
            should_wait = False
            if callable(recalc_in_progress):
                try:
                    should_wait = bool(recalc_in_progress())
                except Exception:
                    should_wait = False
            else:
                remaining = completion_state.get("fallback_waits", 2)
                if remaining > 0:
                    completion_state["fallback_waits"] = remaining - 1
                    should_wait = True
            if should_wait:
                self._call_later(_check, delay_ms)
                return
        if completion_state.get("done"):
            return
        completion_state["done"] = True
        self._run_on_main(self._handle_prioritysieve_recalc_completed)

        self._call_later(_check, delay_ms)

    def _ensure_prioritysieve_completion_hooks(self, ps_main: ModuleType) -> None:
        self._wrap_prioritysieve_completion_hook(ps_main, "_on_success")
        self._wrap_prioritysieve_completion_hook(ps_main, "_on_failure")

    def _wrap_prioritysieve_completion_hook(self, ps_main: ModuleType, attr_name: str) -> None:
        original = getattr(ps_main, attr_name, None)
        if not callable(original):
            return
        if getattr(original, "_kanjicards_completion_wrapper", False):
            return

        manager = self

        @wraps(original)
        def wrapped(*args: object, **kwargs: object) -> object:
            try:
                return original(*args, **kwargs)
            finally:
                manager._run_on_main(manager._handle_prioritysieve_recalc_completed)

        setattr(wrapped, "_kanjicards_completion_wrapper", True)
        setattr(ps_main, attr_name, wrapped)

    def _prioritysieve_post_sync_active(self) -> bool:
        ps_main = self._prioritysieve_recalc_main()
        if ps_main is None:
            return False
        addon_manager = getattr(self.mw, "addonManager", None)
        get_config = getattr(addon_manager, "getConfig", None) if addon_manager else None
        if callable(get_config):
            for module_name in ("prioritysieve", "prioritysieve.__init__"):
                try:
                    raw_config = get_config(module_name)
                except Exception:
                    continue
                if not isinstance(raw_config, dict):
                    continue
                setting = raw_config.get("recalc_after_sync")
                if isinstance(setting, bool):
                    return setting
                if isinstance(setting, str):
                    normalized = setting.strip().lower()
                    if normalized in {"true", "1", "yes", "on"}:
                        return True
                    if normalized in {"false", "0", "no", "off"}:
                        return False
        try:
            config_module = __import__("prioritysieve.prioritysieve_config", fromlist=["PrioritySieveConfig"])
        except Exception:
            return False
        config_cls = getattr(config_module, "PrioritySieveConfig", None)
        if config_cls is None:
            return False
        try:
            config = config_cls()
        except Exception:
            return False
        try:
            setting_value = getattr(config, "recalc_after_sync")
        except Exception:
            return False
        if isinstance(setting_value, bool):
            return setting_value
        if isinstance(setting_value, str):
            normalized_setting = setting_value.strip().lower()
            return normalized_setting in {"true", "1", "yes", "on"}
        return bool(setting_value)

    def _handle_prioritysieve_recalc_completed(self) -> None:
        if getattr(self, "_prioritysieve_toolbar_followup", False):
            self._prioritysieve_toolbar_followup = False
            try:
                self.run_recalc()
            except Exception:
                pass
            return
        if getattr(self, "_prioritysieve_waiting_post_sync", False):
            self._prioritysieve_waiting_post_sync = False
            self.run_after_sync()

    def _on_top_toolbar_init_links(self, links: List[str], toolbar: Toolbar) -> None:
        ps_main = self._prioritysieve_recalc_main()
        for index in range(len(links) - 1, -1, -1):
            if f'id="{KANJICARDS_TOOLBAR_ID}"' in links[index]:
                links.pop(index)
        if ps_main:
            self._maybe_wrap_prioritysieve_recalc(ps_main)
            return
        link = toolbar.create_link(
            cmd=KANJICARDS_TOOLBAR_CMD,
            label="Recalc",
            func=self.run_toolbar_recalc,
            tip="Recalculate Kanji cards",
            id=KANJICARDS_TOOLBAR_ID,
        )
        links.append(link)

    def _on_toolbar_did_redraw(self, toolbar: Toolbar) -> None:
        link_handlers = getattr(toolbar, "link_handlers", None)
        if not isinstance(link_handlers, dict):
            return
        ps_main = self._prioritysieve_recalc_main()
        if ps_main:
            self._maybe_wrap_prioritysieve_recalc(ps_main)
            if PRIORITYSIEVE_TOOLBAR_CMD in link_handlers:
                link_handlers[PRIORITYSIEVE_TOOLBAR_CMD] = self.run_toolbar_recalc
            link_handlers.pop(KANJICARDS_TOOLBAR_CMD, None)
            return
        self._maybe_wrap_prioritysieve_recalc()
        if KANJICARDS_TOOLBAR_CMD in link_handlers:
            link_handlers[KANJICARDS_TOOLBAR_CMD] = self.run_toolbar_recalc

    # ------------------------------------------------------------------
    # Recalc routine
    # ------------------------------------------------------------------
    def run_toolbar_recalc(self) -> None:
        ps_main = self._prioritysieve_recalc_main()
        if ps_main:
            self._maybe_wrap_prioritysieve_recalc(ps_main)
        priority_recalc = getattr(ps_main, "recalc", None) if ps_main else None
        if not callable(priority_recalc):
            self.run_recalc()
            return

        self._prioritysieve_toolbar_followup = True
        try:
            priority_recalc()
        except Exception:
            self._prioritysieve_toolbar_followup = False
            self.run_recalc()
            return

    def run_recalc(self) -> None:
        self.mw.checkpoint("KanjiCards")
        progress_obj = getattr(self.mw, "progress", None)
        self.mw.progress.start(label="Preparing KanjiCards…", immediate=True)
        progress_tracker: Optional[Dict[str, object]] = None
        if progress_obj and hasattr(progress_obj, "update"):
            progress_tracker = {"progress": progress_obj, "current": 0, "max": 5}
            try:
                progress_obj.update(label="Collecting configuration…", value=0, max=5)
            except TypeError:
                try:
                    progress_obj.update(label="Collecting configuration…")
                except TypeError:
                    pass
        cfg = self.load_config()
        try:
            stats = self._recalc_internal(progress_tracker=progress_tracker, cfg=cfg)
        except Exception as err:  # noqa: BLE001
            self._pending_vocab_sync_marker = None
            self._pending_config_hash = None
            self.mw.progress.finish()
            show_critical(f"KanjiCards recalc failed:\n{err}")
            return None
        else:
            self.mw.progress.finish()
            self._commit_vocab_sync_marker(cfg)
            self._notify_summary(stats)
            self.mw.reset()
            return stats

    def _progress_step(self, tracker: Optional[Dict[str, object]], label: str) -> None:
        if not tracker:
            return
        progress = tracker.get("progress")
        update = getattr(progress, "update", None)
        if not callable(update):
            return
        current = int(tracker.get("current", 0)) + 1
        tracker["current"] = current
        max_value = tracker.get("max")
        kwargs: Dict[str, object] = {"label": label}
        if isinstance(max_value, int):
            kwargs["value"] = max(min(current, max_value), 0)
            kwargs["max"] = max_value

        def _do_update() -> None:
            try:
                update(**kwargs)
            except TypeError:
                try:
                    update(label)
                except TypeError:
                    pass

        taskman = getattr(self.mw, "taskman", None)
        if taskman and hasattr(taskman, "run_on_main"):
            try:
                taskman.run_on_main(_do_update)
            except Exception:
                _do_update()
        else:
            _do_update()

        try:
            QApplication.processEvents()
        except Exception:
            pass

    def _recalc_internal(
        self,
        *,
        progress_tracker: Optional[Dict[str, object]] = None,
        cfg: Optional[AddonConfig] = None,
    ) -> Dict[str, object]:
        if cfg is None:
            cfg = self.load_config()
        collection = self.mw.col
        if collection is None:
            raise RuntimeError("Collection not available")

        cfg_hash = self._hash_config(cfg)
        self._pending_config_hash = cfg_hash

        if not cfg.kanji_note_type.name:
            raise RuntimeError("Kanji note type is not configured yet")

        self._progress_step(progress_tracker, "Resolving note types…")
        kanji_model, kanji_field_indexes, kanji_field_index = self._get_kanji_model_context(collection, cfg)

        vocab_models = self._resolve_vocab_models(collection, cfg)
        if not vocab_models:
            raise RuntimeError("No valid vocabulary note types configured")

        self._progress_step(progress_tracker, "Loading dictionary data…")
        dictionary = self._load_dictionary(cfg.dictionary_file)

        self._progress_step(progress_tracker, "Scanning vocabulary notes…")
        usage_info = self._collect_vocab_usage(collection, vocab_models, cfg)
        active_chars = set(usage_info.keys())

        existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)

        self._progress_step(progress_tracker, "Updating kanji cards…")
        stats = self._apply_kanji_updates(
            collection,
            active_chars,
            dictionary,
            kanji_model,
            kanji_field_indexes,
            kanji_field_index,
            cfg,
            usage_info,
            existing_notes,
            prune_existing=True,
        )

        if cfg.reorder_mode in {"frequency", "vocab", "vocab_frequency"}:
            reorder_stats = self._reorder_new_kanji_cards(
                collection,
                kanji_model,
                kanji_field_index,
                cfg,
                usage_info,
                dictionary,
            )
            for key, value in reorder_stats.items():
                try:
                    stats[key] = stats.get(key, 0) + int(value)
                except Exception:
                    continue

        vocab_field_map = {model["id"]: field_indexes for model, field_indexes, _ in vocab_models}
        self._progress_step(progress_tracker, "Updating vocabulary suspension…")
        suspension_stats = self._update_vocab_suspension(
            collection,
            cfg,
            vocab_field_map,
            existing_notes,
        )
        stats["vocab_suspended"] += suspension_stats.get("vocab_suspended", 0)
        stats["vocab_unsuspended"] += suspension_stats.get("vocab_unsuspended", 0)

        marker_count, marker_mod = self._compute_vocab_sync_marker(collection, vocab_models)
        self._pending_vocab_sync_marker = (marker_count, marker_mod)

        return stats

    def _compute_vocab_sync_marker(
        self,
        collection: Collection,
        vocab_models: Sequence[Tuple[NotetypeDict, List[int], float]],
    ) -> Tuple[int, int]:
        model_ids = [
            model["id"]
            for model, _, _ in vocab_models
            if isinstance(model, dict) and isinstance(model.get("id"), int)
        ]
        if not model_ids:
            return 0, 0
        placeholders = ",".join("?" for _ in model_ids)
        try:
            rows = _db_all(
                collection,
                f"SELECT COUNT(*), MAX(mod) FROM notes WHERE mid IN ({placeholders})",
                *model_ids,
                context="compute_vocab_sync_marker",
            )
        except Exception:
            return 0, 0
        if not rows:
            return 0, 0
        count_raw, max_mod_raw = rows[0]
        try:
            count = int(count_raw or 0)
        except Exception:
            count = 0
        try:
            max_mod = int(max_mod_raw or 0)
        except Exception:
            max_mod = 0
        return count, max_mod

    def _have_vocab_notes_changed(self, collection: Collection, cfg: AddonConfig) -> bool:
        if self._last_vocab_sync_mod is None or self._last_vocab_sync_count is None:
            return True
        try:
            vocab_models = self._resolve_vocab_models(collection, cfg)
        except Exception:
            return True
        count, max_mod = self._compute_vocab_sync_marker(collection, vocab_models)
        if count != self._last_vocab_sync_count:
            return True
        if max_mod > self._last_vocab_sync_mod:
            return True
        return False

    def _commit_vocab_sync_marker(self, cfg: AddonConfig) -> None:
        if self._pending_vocab_sync_marker is not None:
            count, max_mod = self._pending_vocab_sync_marker
            self._last_vocab_sync_count = int(count)
            self._last_vocab_sync_mod = int(max_mod)
        if self._pending_config_hash is not None:
            self._last_synced_config_hash = self._pending_config_hash
        self._pending_config_hash = None
        raw = self._serialize_config(cfg)
        self._write_profile_config(raw)
        self._pending_vocab_sync_marker = None

    def _on_reviewer_did_show_question(self, card: Any, *args: Any, **kwargs: Any) -> None:
        if not card:
            return
        card_id = getattr(card, "id", None)
        if card_id is None or not isinstance(card_id, int):
            if isinstance(card_id, (str, bytes)):
                try:
                    numeric_id = int(card_id)
                except Exception:
                    numeric_id = None
                else:
                    self._debug("realtime/fallback_card_id", original=card_id, fallback=numeric_id, source="string")
                    card_id = numeric_id
            if card_id is None or not isinstance(card_id, int):
                if self._last_question_card_id is not None and self._last_question_card_id in self._pre_answer_card_state:
                    self._debug(
                        "realtime/fallback_card_id",
                        original=card_id,
                        fallback=self._last_question_card_id,
                        source="last_question",
                    )
                    card_id = self._last_question_card_id
                elif self._pre_answer_card_state:
                    # Use the most recent stored key.
                    fallback_id = next(reversed(self._pre_answer_card_state))
                    self._debug(
                        "realtime/fallback_card_id",
                        original=card_id,
                        fallback=fallback_id,
                        source="stored_state",
                    )
                    card_id = fallback_id
                elif hasattr(card, "card"):
                    candidate = getattr(card, "card", None)
                    fallback_id = getattr(candidate, "id", None)
                    self._debug(
                        "realtime/fallback_card_id",
                        original=card_id,
                        fallback=fallback_id,
                        source="card_attr",
                    )
                    card_id = fallback_id
        if card_id is None or not isinstance(card_id, int):
            self._debug("realtime/skip", reason="missing_card_id")
            return
        card_type = getattr(card, "type", None)
        stored_type = card_type if isinstance(card_type, int) else None
        queue = getattr(card, "queue", None)
        stored_queue = queue if isinstance(queue, int) else None
        note_id = getattr(card, "nid", None)
        stored_note_id = note_id if isinstance(note_id, int) else None
        self._pre_answer_card_state[card_id] = {
            "type": stored_type,
            "queue": stored_queue,
            "note_id": stored_note_id,
        }
        self._last_question_card_id = card_id
        self._debug(
            "realtime/question",
            card_id=card_id,
            card_type=stored_type,
            queue=stored_queue,
            note_id=stored_note_id,
        )

    def _on_reviewer_did_answer_card(self, card: Any, *args: Any, **kwargs: Any) -> None:
        if not card:
            return
        if self.mw.col is None:
            return
        self._debug(
            "realtime/did_answer",
            card_id=getattr(card, "id", None),
            queue=getattr(card, "queue", None),
            type=getattr(card, "type", None),
        )
        try:
            self._process_reviewed_card(card)
        except Exception as err:  # noqa: BLE001
            if not self._realtime_error_logged:
                _safe_print(f"[KanjiCards] realtime recalc error: {err}")
                self._realtime_error_logged = True

    def _process_reviewed_card(self, card: Any) -> None:
        collection = self.mw.col
        if collection is None:
            return

        card_id_obj = getattr(card, "id", None)
        card_id: Optional[int]
        if isinstance(card_id_obj, int):
            card_id = card_id_obj
        elif isinstance(card_id_obj, (str, bytes)):
            try:
                card_id = int(card_id_obj)
            except Exception:
                card_id = None
            else:
                self._debug(
                    "realtime/fallback_card_id",
                    original=card_id_obj,
                    fallback=card_id,
                    source="string",
                )
        else:
            card_id = None

        if card_id is None or card_id not in self._pre_answer_card_state:
            if self._last_question_card_id is not None and self._last_question_card_id in self._pre_answer_card_state:
                self._debug(
                    "realtime/fallback_card_id",
                    original=card_id_obj,
                    fallback=self._last_question_card_id,
                    source="last_question",
                )
                card_id = self._last_question_card_id
            elif self._pre_answer_card_state:
                fallback_id = next(reversed(self._pre_answer_card_state))
                self._debug(
                    "realtime/fallback_card_id",
                    original=card_id_obj,
                    fallback=fallback_id,
                    source="stored_state",
                )
                card_id = fallback_id
            elif hasattr(card, "card"):
                candidate = getattr(card, "card", None)
                fallback_id = getattr(candidate, "id", None)
                if isinstance(fallback_id, int):
                    self._debug(
                        "realtime/fallback_card_id",
                        original=card_id_obj,
                        fallback=fallback_id,
                        source="card_attr",
                    )
                    card_id = fallback_id

        if card_id is None or card_id not in self._pre_answer_card_state:
            self._debug(
                "realtime/skip",
                reason="missing_card_id",
                original=card_id_obj,
            )
            return

        cfg = self.load_config()
        if not cfg.realtime_review:
            self._debug("realtime/skip", reason="realtime_disabled")
            return

        try:
            kanji_model, _, kanji_field_index = self._get_kanji_model_context(collection, cfg)
        except RuntimeError:
            # Configuration incomplete; wait until user configures properly.
            self._debug("realtime/skip", reason="kanji_model_unavailable")
            return

        prev_state = self._pre_answer_card_state.pop(card_id, None)
        if self._last_question_card_id == card_id:
            self._last_question_card_id = None
        if prev_state is None:
            self._debug("realtime/skip", reason="missing_pre_state", card_id=card_id)
            return
        prev_type = prev_state.get("type")
        prev_queue = prev_state.get("queue")
        note_id_hint = prev_state.get("note_id")

        note: Optional[Note]
        try:
            note = card.note()
        except Exception:  # noqa: BLE001
            note = None
            self._debug(
                "realtime/skip",
                reason="note_lookup_failed_fetch",
                card_id=card_id,
                note_id_hint=note_id_hint,
            )
            if note_id_hint:
                try:
                    note = self.mw.col.get_note(note_id_hint)
                except Exception as err:  # noqa: BLE001
                    self._debug(
                        "realtime/skip",
                        reason="note_lookup_failed_hint",
                        card_id=card_id,
                        note_id=note_id_hint,
                        error=str(err),
                    )
                    note = None
            if note is None:
                fetched_card = None
                try:
                    fetched_card = self.mw.col.get_card(card_id)
                except Exception as err:  # noqa: BLE001
                    self._debug(
                        "realtime/skip",
                        reason="card_lookup_failed",
                        card_id=card_id,
                        error=str(err),
                    )
                if fetched_card is not None:
                    try:
                        note = fetched_card.note()
                    except Exception as err:  # noqa: BLE001
                        self._debug(
                            "realtime/skip",
                            reason="note_lookup_failed",
                            card_id=card_id,
                            error=str(err),
                        )
                        note = None
            if note is None:
                return

        if note.mid != kanji_model.get("id"):
            self._debug(
                "realtime/skip",
                reason="not_kanji_note",
                note_mid=note.mid,
                kanji_mid=kanji_model.get("id"),
            )
            return

        try:
            fields = list(note.fields)
        except Exception:  # noqa: BLE001
            fields = note.split_fields() if hasattr(note, "split_fields") else []

        kanji_chars: Set[str] = set()
        if kanji_field_index < len(fields):
            kanji_chars.update(KANJI_PATTERN.findall(fields[kanji_field_index]))

        if not kanji_chars:
            self._debug("realtime/skip", reason="no_kanji_chars", card_id=card_id)
            return

        if not cfg.vocab_note_types:
            self._debug("realtime/skip", reason="no_vocab_types")
            return

        vocab_map = self._get_vocab_model_map(collection, cfg)
        if not vocab_map:
            self._debug("realtime/skip", reason="vocab_map_empty")
            return

        vocab_field_map = {model["id"]: indexes for model, indexes, _ in vocab_map.values()}
        note_id_value = getattr(note, "id", note_id_hint)
        self._debug(
            "realtime/process",
            card_id=card_id,
            note_id=note_id_value,
            chars="".join(sorted(kanji_chars)),
            prev_type=prev_type,
            prev_queue=prev_queue,
        )

        existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)
        if not existing_notes:
            self._debug("realtime/skip", reason="no_existing_notes")
            return

        self._update_vocab_suspension(
            collection,
            cfg,
            vocab_field_map,
            existing_notes,
            target_chars=kanji_chars,
        )

        self._realtime_error_logged = False

    def _on_sync_event(self, *args: Any, **kwargs: Any) -> None:
        if self._prioritysieve_post_sync_active():
            self._prioritysieve_waiting_post_sync = True
            return
        self._prioritysieve_waiting_post_sync = False
        self.run_after_sync()

    def run_after_sync(
        self,
        *,
        allow_followup: bool = True,
        on_finished: Optional[Callable[[bool], None]] = None,
    ) -> None:
        callback = on_finished or (lambda _changed: None)
        self._prioritysieve_waiting_post_sync = False
        if self._suppress_next_auto_sync:
            self._suppress_next_auto_sync = False
            callback(False)
            return

        cfg = self.load_config()
        if not cfg.auto_run_on_sync:
            callback(False)
            return
        if not self.mw or not self.mw.col:
            callback(False)
            return

        collection = self.mw.col
        config_hash = self._hash_config(cfg)
        config_changed = self._last_synced_config_hash != config_hash
        vocab_changed = False
        if not config_changed:
            vocab_changed = self._have_vocab_notes_changed(collection, cfg)
        if not config_changed and not vocab_changed:
            callback(False)
            return

        def execute() -> None:
            if not self.mw or not self.mw.col:
                callback(False)
                return
            self._realtime_error_logged = False
            stats = self.run_recalc()
            changed = bool(stats and self._stats_warrant_sync(stats))
            if changed and allow_followup:

                def followup() -> None:
                    if self._trigger_followup_sync():
                        self._suppress_next_auto_sync = True

                QTimer.singleShot(200, followup)
            callback(changed)

        busy_check = getattr(self.mw.progress, "busy", None)
        if callable(busy_check) and busy_check():
            QTimer.singleShot(
                200,
                lambda: self.run_after_sync(
                    allow_followup=allow_followup,
                    on_finished=callback,
                ),
            )
            return

        taskman = getattr(self.mw, "taskman", None)
        run_on_main = getattr(taskman, "run_on_main", None)
        if callable(run_on_main):
            try:
                run_on_main(execute)
                return
            except Exception:
                pass
        execute()

    def mark_followup_sync_scheduled(self) -> None:
        self._suppress_next_auto_sync = True

    def _stats_warrant_sync(self, stats: Dict[str, object]) -> bool:
        for key in (
            "created",
            "existing_tagged",
            "unsuspended",
            "tag_removed",
            "resuspended",
            "vocab_suspended",
            "vocab_unsuspended",
            "cards_reordered",
            "bucket_tags_updated",
        ):
            try:
                if int(stats.get(key, 0)) > 0:
                    return True
            except Exception:
                continue
        return False

    def _trigger_followup_sync(self) -> bool:
        methods = [
            "on_sync_button_clicked",
            "onSyncButton",
            "onSync",
            "on_sync_clicked",
        ]
        for name in methods:
            handler = getattr(self.mw, name, None)
            if callable(handler):
                handler()
                return True
        toolbar = getattr(self.mw, "form", None)
        sync_button = getattr(toolbar, "syncButton", None)
        if sync_button is not None:
            try:
                sync_button.animateClick()
                return True
            except Exception:
                return False
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_kanji_model_context(
        self,
        collection: Collection,
        cfg: AddonConfig,
    ) -> Tuple[NotetypeDict, Dict[str, int], int]:
        if not cfg.kanji_note_type.name:
            raise RuntimeError("Kanji note type is not configured yet")

        key = (
            cfg.kanji_note_type.name,
            tuple(sorted(cfg.kanji_note_type.fields.items())),
        )
        cache = self._kanji_model_cache
        if cache and cache.get("key") == key:
            return (
                cache["model"],
                cache["field_indexes"],
                cache["kanji_field_index"],
            )

        kanji_model = collection.models.byName(cfg.kanji_note_type.name)
        if kanji_model is None:
            raise RuntimeError(f"Kanji note type '{cfg.kanji_note_type.name}' was not found")
        kanji_field_indexes = self._resolve_field_indexes(kanji_model, cfg.kanji_note_type.fields)
        kanji_field_name = cfg.kanji_note_type.fields.get("kanji", "")
        if not kanji_field_name:
            raise RuntimeError("Kanji field mapping is missing in the Kanji note configuration")
        kanji_field_index = kanji_field_indexes.get("kanji")
        if kanji_field_index is None:
            raise RuntimeError(
                f"Kanji field '{kanji_field_name}' was not found in note type '{kanji_model['name']}'"
            )

        self._kanji_model_cache = {
            "key": key,
            "model": kanji_model,
            "field_indexes": kanji_field_indexes,
            "kanji_field_index": kanji_field_index,
        }
        self._existing_notes_cache = None

        return kanji_model, kanji_field_indexes, kanji_field_index

    def _resolve_field_indexes(
        self,
        model: NotetypeDict,
        mapping: Dict[str, str],
    ) -> Dict[str, int]:
        name_to_index = {fld["name"]: idx for idx, fld in enumerate(model["flds"])}
        result: Dict[str, int] = {}
        for logical_name, field_name in mapping.items():
            if not field_name:
                continue
            if field_name not in name_to_index:
                raise RuntimeError(
                    f"Field '{field_name}' configured for '{logical_name}' does not exist in note type '{model['name']}'"
                )
            result[logical_name] = name_to_index[field_name]
        return result

    def _resolve_vocab_models(
        self,
        collection: Collection,
        cfg: AddonConfig,
    ) -> List[Tuple[NotetypeDict, List[int], float]]:
        vocab_models: List[Tuple[NotetypeDict, List[int], float]] = []
        for vocab_cfg in cfg.vocab_note_types:
            if not vocab_cfg.name:
                continue
            model = collection.models.byName(vocab_cfg.name)
            if model is None:
                continue
            field_indexes = []
            name_to_index = {fld["name"]: idx for idx, fld in enumerate(model["flds"])}
            fields_missing = [f for f in vocab_cfg.fields if f not in name_to_index]
            if fields_missing:
                continue
            for fname in vocab_cfg.fields:
                field_indexes.append(name_to_index[fname])
            multiplier = vocab_cfg.due_multiplier if vocab_cfg.due_multiplier > 0 else 1.0
            try:
                multiplier = float(multiplier)
            except Exception:
                multiplier = 1.0
            if multiplier <= 0:
                multiplier = 1.0
            vocab_models.append((model, field_indexes, multiplier))
        return vocab_models

    def _get_vocab_model_map(
        self,
        collection: Collection,
        cfg: AddonConfig,
    ) -> Dict[int, Tuple[NotetypeDict, List[int], float]]:
        key = tuple(
            sorted(
                (entry.name, tuple(entry.fields), float(entry.due_multiplier))
                for entry in cfg.vocab_note_types
                if entry.name
            )
        )
        cache = self._vocab_model_cache
        if cache and cache.get("key") == key:
            return cache["mapping"]

        vocab_models = self._resolve_vocab_models(collection, cfg)
        mapping = {model["id"]: (model, field_indexes, multiplier) for model, field_indexes, multiplier in vocab_models}
        self._vocab_model_cache = {"key": key, "mapping": mapping}
        return mapping

    def _load_dictionary(self, file_name: str) -> Dict[str, Dict[str, object]]:
        if not file_name:
            raise RuntimeError("Dictionary file path is not configured")
        path = file_name
        if not os.path.isabs(path):
            path = os.path.join(self.addon_dir, path)
        if not os.path.exists(path):
            raise RuntimeError(f"Dictionary file not found at '{path}'")
        lower_path = path.lower()
        mtime = os.path.getmtime(path)
        cache = self._dictionary_cache
        if cache and cache.get("path") == path and cache.get("mtime") == mtime:
            return cache["data"]

        if lower_path.endswith(".json"):
            data = self._load_dictionary_json(path)
        elif lower_path.endswith(".xml"):
            data = self._load_dictionary_kanjidic(path)
        else:
            try:
                data = self._load_dictionary_kanjidic(path)
            except Exception:
                data = self._load_dictionary_json(path)

        self._dictionary_cache = {"path": path, "mtime": mtime, "data": data}
        return data

    def _load_dictionary_json(self, path: str) -> Dict[str, Dict[str, object]]:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise RuntimeError("Dictionary file must contain a JSON object mapping kanji to data")
        for key, value in data.items():
            if not isinstance(value, dict):
                continue
            freq_val = value.get("frequency")
            if isinstance(freq_val, str) and freq_val.isdigit():
                value["frequency"] = int(freq_val)
            elif isinstance(freq_val, (int, float)):
                value["frequency"] = int(freq_val)
            elif "frequency" in value:
                value["frequency"] = None
        return data

    def _load_dictionary_kanjidic(self, path: str) -> Dict[str, Dict[str, object]]:
        try:
            tree = ET.parse(path)
        except ET.ParseError as err:
            raise RuntimeError(
                "Dictionary XML could not be parsed; ensure it is a valid KANJIDIC2 file"
            ) from err
        root = tree.getroot()
        if root is None or root.tag != "kanjidic2":
            raise RuntimeError("Dictionary XML does not appear to be a KANJIDIC2 file")

        dictionary: Dict[str, Dict[str, object]] = {}
        for character in root.findall("character"):
            literal = (character.findtext("literal") or "").strip()
            if not literal:
                continue

            misc = character.find("misc")
            stroke_value = ""
            if misc is not None:
                stroke_el = misc.find("stroke_count")
                if stroke_el is not None and stroke_el.text:
                    stroke_text = stroke_el.text.strip()
                    if stroke_text.isdigit():
                        stroke_value = int(stroke_text)
                    else:
                        stroke_value = stroke_text

            reading_meaning = character.find("reading_meaning")
            kunyomi: List[str] = []
            onyomi: List[str] = []
            meanings: List[str] = []

            if reading_meaning is not None:
                for rmgroup in reading_meaning.findall("rmgroup"):
                    for reading in rmgroup.findall("reading"):
                        text = (reading.text or "").strip()
                        if not text:
                            continue
                        r_type = reading.get("r_type") or ""
                        if r_type == "ja_kun":
                            kunyomi.append(text)
                        elif r_type == "ja_on":
                            onyomi.append(text)
                    for meaning in rmgroup.findall("meaning"):
                        text = (meaning.text or "").strip()
                        if not text:
                            continue
                        lang = meaning.get("m_lang")
                        if lang and lang not in {"", "en"}:
                            continue
                        meanings.append(text)

            frequency_value: Optional[int] = None
            if misc is not None:
                freq_el = misc.find("freq")
                if freq_el is not None and freq_el.text:
                    freq_text = freq_el.text.strip()
                    if freq_text.isdigit():
                        frequency_value = int(freq_text)

            entry = {
                "definition": "; ".join(dict.fromkeys(meanings)),
                "stroke_count": stroke_value,
                "kunyomi": list(dict.fromkeys(kunyomi)),
                "onyomi": list(dict.fromkeys(onyomi)),
                "frequency": frequency_value,
            }
            dictionary[literal] = entry

        if not dictionary:
            raise RuntimeError("No kanji entries were parsed from the dictionary XML")
        return dictionary

    def _collect_vocab_usage(
        self,
        collection: Collection,
        vocab_models: Sequence[Tuple[NotetypeDict, List[int], float]],
        cfg: AddonConfig,
    ) -> Dict[str, KanjiUsageInfo]:
        usage: Dict[str, KanjiUsageInfo] = {}

        def _safe_int(value: object) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(value)  # type: ignore[arg-type]
            except Exception:
                return None

        all_rows: List[
            Tuple[
                int,
                str,
                Set[str],
                bool,
                Optional[int],
                Optional[int],
                Tuple[int, ...],
            ]
        ] = []

        for model, field_indexes, multiplier in vocab_models:
            if not field_indexes:
                continue
            try:
                multiplier_value = float(multiplier)
            except Exception:
                multiplier_value = 1.0
            if multiplier_value <= 0:
                multiplier_value = 1.0
            sql = (
                "SELECT notes.id, notes.flds, notes.tags, "
                "MAX(CASE WHEN cards.type != 0 THEN 1 ELSE 0 END) AS has_reviewed, "
                "MIN(CASE WHEN cards.queue = 0 THEN cards.due END) AS min_new_due, "
                "MIN(CASE WHEN cards.queue = -1 THEN cards.due END) AS min_suspended_due, "
                "MIN(CASE WHEN cards.type != 0 THEN cards.due END) AS min_review_due "
                "FROM notes JOIN cards ON cards.nid = notes.id "
                "WHERE notes.mid = ? GROUP BY notes.id"
            )
            rows = _db_all(
                collection,
                sql,
                model["id"],
                context=f"collect_vocab_usage:{model.get('name')}",
            )
            auto_suspend_tag = cfg.auto_suspend_tag.strip()
            auto_suspend_tag_lower = auto_suspend_tag.lower()
            prepared_rows: List[
                Tuple[int, str, Set[str], bool, Optional[int], Optional[int]]
            ] = []
            for (
                note_id,
                flds,
                tags_text,
                has_reviewed,
                min_new_due,
                min_suspended_due,
                min_review_due,
            ) in rows:
                tags_string = tags_text or ""
                note_tags_lower = {tag.lower() for tag in tags_string.split() if tag}
                new_due_value = _safe_int(min_new_due)
                suspended_due_value = _safe_int(min_suspended_due)
                if (
                    auto_suspend_tag_lower
                    and auto_suspend_tag_lower in note_tags_lower
                    and suspended_due_value is not None
                    and (new_due_value is None or suspended_due_value < new_due_value)
                ):
                    new_due_value = suspended_due_value
                if new_due_value is not None:
                    try:
                        scaled_value = int(round(new_due_value * multiplier_value))
                    except Exception:
                        scaled_value = new_due_value
                    if scaled_value < 0:
                        scaled_value = 0
                    new_due_value = scaled_value
                review_due_value = _safe_int(min_review_due)
                prepared_rows.append(
                    (note_id, flds, note_tags_lower, bool(has_reviewed), new_due_value, review_due_value)
                )

            active_map: Dict[int, bool] = {}
            if cfg.ignore_suspended_vocab and prepared_rows:
                note_ids = [row[0] for row in prepared_rows]
                active_map = self._load_note_active_status(collection, note_ids)

            field_indexes_tuple = tuple(field_indexes)
            for note_id, flds, note_tags_lower, reviewed_flag, new_due_value, review_due_value in prepared_rows:
                if cfg.ignore_suspended_vocab:
                    has_active = active_map.get(note_id, False)
                    if not has_active:
                        if not auto_suspend_tag_lower or auto_suspend_tag_lower not in note_tags_lower:
                            continue
                all_rows.append(
                    (
                        note_id,
                        flds,
                        note_tags_lower,
                        reviewed_flag,
                        new_due_value,
                        review_due_value,
                        field_indexes_tuple,
                    )
                )

        if not all_rows:
            return usage

        big = 10**9
        new_rows = [row for row in all_rows if row[4] is not None]
        new_rank_map: Dict[int, int] = {}
        if new_rows:
            new_rows.sort(key=lambda row: (row[4], row[0]))  # type: ignore[arg-type]
            for idx, row in enumerate(new_rows):
                new_rank_map[row[0]] = idx

        review_rows = [row for row in all_rows if row[3]]
        review_rank_map: Dict[int, int] = {}
        if review_rows:
            review_rows.sort(key=lambda row: ((row[5] if row[5] is not None else big), row[0]))
            for idx, row in enumerate(review_rows):
                review_rank_map[row[0]] = idx

        all_rows.sort(
            key=lambda row: (
                0 if row[4] is not None else 1,
                row[4] if row[4] is not None else big,
                row[0],
            )
        )

        for note_id, flds, _note_tags_lower, reviewed_flag, new_due_value, review_due_value, field_indexes_tuple in all_rows:
            review_rank = review_rank_map.get(note_id)
            new_rank = new_rank_map.get(note_id)
            fields = flds.split("\x1f")
            seen_in_note: Set[str] = set()
            for field_index in field_indexes_tuple:
                if field_index >= len(fields):
                    continue
                value = fields[field_index]
                chars = KANJI_PATTERN.findall(value)
                if not chars:
                    continue
                for char in chars:
                    info = usage.get(char)
                    if info is None:
                        info = KanjiUsageInfo()
                        usage[char] = info
                    if char not in seen_in_note:
                        info.vocab_occurrences += 1
                        seen_in_note.add(char)
                    if reviewed_flag:
                        info.reviewed = True
                        if review_rank is not None and (
                            info.first_review_order is None or review_rank < info.first_review_order
                        ):
                            info.first_review_order = review_rank
                        if review_due_value is not None and (
                            info.first_review_due is None or review_due_value < info.first_review_due
                        ):
                            info.first_review_due = review_due_value
                    if new_due_value is not None and (
                        info.first_new_due is None or new_due_value < info.first_new_due
                    ):
                        info.first_new_due = new_due_value
                    if new_rank is not None and (
                        info.first_new_order is None or new_rank < info.first_new_order
                    ):
                        info.first_new_order = new_rank
        return usage

    def _notify_summary(self, stats: Dict[str, object]) -> None:
        try:
            scanned = int(stats.get("kanji_scanned", 0))
        except Exception:
            scanned = 0
        created = int(stats.get("created", 0))
        tagged = int(stats.get("existing_tagged", 0))
        unsuspended = int(stats.get("unsuspended", 0))
        removed = int(stats.get("tag_removed", 0))
        resuspended = int(stats.get("resuspended", 0))
        vocab_suspended = int(stats.get("vocab_suspended", 0))
        vocab_unsuspended = int(stats.get("vocab_unsuspended", 0))

        lines = [
            f"Scanned: {scanned}",
            f"Tagged: {tagged}",
            f"Created: {created}",
        ]
        if unsuspended:
            lines.append(f"Unsuspended: {unsuspended}")
        if removed:
            lines.append(f"Tags removed: {removed}")
        if resuspended:
            lines.append(f"Resuspended: {resuspended}")
        if vocab_suspended:
            lines.append(f"Vocab suspended: {vocab_suspended}")
        if vocab_unsuspended:
            lines.append(f"Vocab unsuspended: {vocab_unsuspended}")

        missing = stats.get("missing_dictionary")
        if missing:
            try:
                missing_list = sorted(missing)
            except Exception:
                missing_list = []
            preview = ", ".join(missing_list[:5]) if missing_list else ""
            more = "…" if missing_list and len(missing_list) > 5 else ""
            lines.append(
                f"Missing dictionary entries: {len(missing_list)}"
                + (f" ({preview}{more})" if preview else "")
            )

        message = "<br>".join(lines)
        tooltip(f"KanjiCards<br>{message}", parent=self.mw, period=5000)

    def _index_existing_kanji_notes(
        self,
        collection: Collection,
        kanji_model: NotetypeDict,
        kanji_field_index: int,
    ) -> Dict[str, int]:
        rows = _db_all(
            collection,
            "SELECT id, flds FROM notes WHERE mid = ?",
            kanji_model["id"],
            context="index_existing_kanji_notes",
        )
        mapping: Dict[str, int] = {}
        for note_id, flds in rows:
            fields = flds.split("\x1f")
            if kanji_field_index >= len(fields):
                continue
            value = fields[kanji_field_index].strip()
            if value and value not in mapping:
                mapping[value] = note_id
        return mapping

    def _get_existing_kanji_notes(
        self,
        collection: Collection,
        kanji_model: NotetypeDict,
        kanji_field_index: int,
    ) -> Dict[str, int]:
        key = (kanji_model["id"], kanji_field_index)
        cache = self._existing_notes_cache
        if cache and cache.get("key") == key:
            return cache["mapping"]
        mapping = self._index_existing_kanji_notes(collection, kanji_model, kanji_field_index)
        self._existing_notes_cache = {"key": key, "mapping": mapping}
        return mapping

    def _ensure_note_tagged(self, collection: Collection, note_id: int, tag: str) -> Tuple[bool, Note]:
        note = _get_note(collection, note_id)
        if not tag:
            return False, note
        if tag in note.tags:
            return False, note
        _add_tag(note, tag)
        note.flush()
        return True, note

    def _apply_bucket_tag_to_note(
        self,
        collection: Collection,
        note_id: int,
        bucket_id: Optional[int],
        bucket_tag_map: Dict[int, str],
        active_bucket_tags: Set[str],
    ) -> bool:
        target_tag = bucket_tag_map.get(bucket_id, "") if bucket_id is not None else ""
        target_lower = target_tag.lower() if target_tag else ""

        if not target_tag and not active_bucket_tags:
            return False

        note = _get_note(collection, note_id)
        changed = False

        for tag in active_bucket_tags:
            if not tag:
                continue
            if target_lower and tag.lower() == target_lower:
                continue
            if _remove_tag_case_insensitive(note, tag):
                changed = True

        if target_tag:
            existing_lower = {tag.lower() for tag in note.tags}
            if target_lower not in existing_lower:
                _add_tag(note, target_tag)
                changed = True

        if changed:
            note.flush()
        return changed

    def _find_notes_with_bucket_tags(
        self,
        collection: Collection,
        active_bucket_tags: Set[str],
    ) -> Set[int]:
        if not active_bucket_tags:
            return set()

        clauses = []
        params: List[object] = []
        for tag in active_bucket_tags:
            if not tag:
                continue
            clauses.append("tags LIKE ?")
            params.append(f"%{tag}%")
        if not clauses:
            return set()

        sql = f"SELECT id, tags FROM notes WHERE {' OR '.join(clauses)}"
        rows = _db_all(
            collection,
            sql,
            *params,
            context="find_notes_with_bucket_tags",
        )

        tag_lower_map = {tag.lower(): tag for tag in active_bucket_tags if tag}
        result: Set[int] = set()
        for note_id, tags in rows:
            tag_set = {value for value in tags.strip().split() if value}
            lower_values = {value.lower() for value in tag_set}
            if lower_values & set(tag_lower_map.keys()):
                result.add(note_id)
        return result

    def _update_kanji_status_tags(
        self,
        note: Note,
        cfg: AddonConfig,
        has_vocab: bool,
        has_reviewed_vocab: bool,
    ) -> None:
        only_new_tag = cfg.only_new_vocab_tag.strip()
        no_vocab_tag = cfg.no_vocab_tag.strip()
        desired: Set[str] = set()
        if only_new_tag and has_vocab and not has_reviewed_vocab:
            desired.add(only_new_tag)
        if no_vocab_tag and not has_vocab:
            desired.add(no_vocab_tag)

        changed = False

        for tag in (only_new_tag, no_vocab_tag):
            if not tag or tag in desired:
                continue
            if _remove_tag_case_insensitive(note, tag):
                changed = True

        if desired:
            existing_lower = {tag.lower() for tag in note.tags}
            for tag in desired:
                if tag.lower() not in existing_lower:
                    _add_tag(note, tag)
                    changed = True
                    existing_lower.add(tag.lower())

        if changed:
            note.flush()

    def _remove_unused_tags(
        self,
        collection: Collection,
        existing_notes: Dict[str, int],
        tag: str,
        unsuspend_tag: str,
        active_chars: Set[str],
        cfg: AddonConfig,
        frequency_field_name: Optional[str] = None,
        dictionary: Optional[Dict[str, Dict[str, object]]] = None,
        scheduling_field_name: Optional[str] = None,
    ) -> Tuple[int, int]:
        removed = 0
        resuspended_total = 0
        created_tag = cfg.created_tag.strip()
        created_tag_lower = created_tag.lower() if created_tag else ""
        unsuspend_clean = (unsuspend_tag or "").strip()
        unsuspend_lower = unsuspend_clean.lower() if unsuspend_clean else ""
        for kanji_char, note_id in existing_notes.items():
            if kanji_char in active_chars:
                continue
            note = _get_note(collection, note_id)
            changed = False
            entry_obj: Optional[Dict[str, object]] = None
            if dictionary:
                candidate = dictionary.get(kanji_char)
                if isinstance(candidate, dict):
                    entry_obj = candidate
                    if frequency_field_name:
                        if self._update_frequency_field(note, frequency_field_name, candidate.get("frequency")):
                            changed = True
            if self._update_scheduling_info_field(
                note,
                scheduling_field_name,
                kanji_char,
                cfg,
                entry_obj,
                None,
            ):
                changed = True
            tag_lookup = {existing.lower(): existing for existing in note.tags if isinstance(existing, str)}
            if tag in note.tags:
                _remove_tag(note, tag)
                changed = True
                removed += 1
            resuspend_needed = False
            if unsuspend_lower and unsuspend_lower in tag_lookup:
                _remove_tag(note, tag_lookup[unsuspend_lower])
                changed = True
                resuspend_needed = True
            if created_tag_lower and created_tag_lower in tag_lookup:
                resuspend_needed = True
            if resuspend_needed:
                resuspended = _resuspend_note_cards(collection, note)
                if resuspended:
                    resuspended_total += resuspended
            self._update_kanji_status_tags(
                note,
                cfg,
                has_vocab=False,
                has_reviewed_vocab=False,
            )
            if changed:
                note.flush()
        return removed, resuspended_total

    def _reorder_new_kanji_cards(
        self,
        collection: Collection,
        kanji_model: NotetypeDict,
        kanji_field_index: int,
        cfg: AddonConfig,
        usage_info: Dict[str, KanjiUsageInfo],
        dictionary: Dict[str, Dict[str, object]],
    ) -> Dict[str, int]:
        mode = cfg.reorder_mode
        if mode not in {"frequency", "vocab", "vocab_frequency"}:
            return {"cards_reordered": 0, "bucket_tags_updated": 0}

        rows = _db_all(
            collection,
            """
            SELECT cards.id, cards.nid, cards.due, cards.did, cards.mod, cards.usn, notes.flds
            FROM cards
            JOIN notes ON notes.id = cards.nid
            WHERE notes.mid = ? AND cards.queue = 0
            """,
            kanji_model["id"],
            context="reorder_new_kanji_cards/load",
        )
        if not rows:
            return {"cards_reordered": 0, "bucket_tags_updated": 0}

        bucket_tag_map = {
            0: cfg.bucket_tags.get("reviewed_vocab", "").strip(),
            1: cfg.bucket_tags.get("unreviewed_vocab", "").strip(),
            2: cfg.bucket_tags.get("no_vocab", "").strip(),
        }
        active_bucket_tags = {tag for tag in bucket_tag_map.values() if tag}
        apply_bucket_tags = bool(active_bucket_tags)

        entries: List[Tuple[Tuple, int, int, int, int, int, int]] = []
        for card_id, note_id, due_value, deck_id, original_mod, original_usn, flds in rows:
            fields = flds.split("\x1f")
            if kanji_field_index >= len(fields):
                continue
            kanji_char = fields[kanji_field_index].strip()
            if not kanji_char:
                continue
            info = usage_info.get(kanji_char)
            has_vocab = True
            if info is None:
                info = KanjiUsageInfo()
                has_vocab = False
            entry = dictionary.get(kanji_char) or {}
            freq_val = entry.get("frequency")
            freq = None
            if isinstance(freq_val, int):
                freq = freq_val
            elif isinstance(freq_val, str) and freq_val.isdigit():
                freq = int(freq_val)

            key, bucket_id = self._build_reorder_key(
                mode,
                info,
                freq,
                due_value,
                card_id,
                has_vocab,
            )
            entries.append((key, card_id, due_value, original_mod, original_usn, note_id, bucket_id))

        if not entries:
            return {"cards_reordered": 0, "bucket_tags_updated": 0}

        now = intTime()
        usn = collection.usn()
        entries.sort(key=lambda item: item[0])
        processed_notes: Set[int] = set()
        reordered_cards = 0
        bucket_updates = 0
        for new_due, (key, card_id, original_due, original_mod, original_usn, note_id, bucket_id) in enumerate(entries):
            due_changed = new_due != original_due
            if due_changed:
                reordered_cards += 1
            new_mod = now if due_changed else original_mod
            new_usn = usn if due_changed else original_usn
            _db_execute(
                collection,
                "UPDATE cards SET due = ?, mod = ?, usn = ? WHERE id = ?",
                new_due,
                new_mod,
                new_usn,
                card_id,
                context="reorder_new_kanji_cards/update",
            )
            if apply_bucket_tags and note_id not in processed_notes:
                if self._apply_bucket_tag_to_note(
                    collection,
                    note_id,
                    bucket_id,
                    bucket_tag_map,
                    active_bucket_tags,
                ):
                    bucket_updates += 1
                processed_notes.add(note_id)

        if apply_bucket_tags:
            tagged_notes = self._find_notes_with_bucket_tags(collection, active_bucket_tags)
            for note_id in tagged_notes:
                if note_id in processed_notes:
                    continue
                if self._apply_bucket_tag_to_note(
                    collection,
                    note_id,
                    None,
                    bucket_tag_map,
                    active_bucket_tags,
                ):
                    bucket_updates += 1

        return {
            "cards_reordered": reordered_cards,
            "bucket_tags_updated": bucket_updates,
        }

    def _build_reorder_key(
        self,
        mode: str,
        info: KanjiUsageInfo,
        frequency: Optional[int],
        due_value: Optional[int],
        card_id: int,
        has_vocab: bool,
    ) -> Tuple[Tuple, int]:
        big = 10**9
        review_due = info.first_review_due if info.first_review_due is not None else big
        new_due_raw = info.first_new_due
        vocab_due = new_due_raw if new_due_raw is not None else big
        due_sort = due_value if due_value is not None else big
        has_frequency = frequency is not None
        freq_value = frequency if has_frequency else big
        vocab_count = info.vocab_occurrences if has_vocab else 0
        review_rank = info.first_review_order if info.first_review_order is not None else big
        new_rank = info.first_new_order if info.first_new_order is not None else big

        if has_vocab and info.reviewed:
            sort_tuple: Tuple = (
                0,
                review_due,
                -vocab_count,
                freq_value,
                review_rank,
                due_sort,
                card_id,
            )
            bucket_id = 0
        elif has_vocab:
            sort_tuple = (
                1,
                vocab_due,
                -vocab_count,
                freq_value,
                new_rank,
                due_sort,
                card_id,
            )
            bucket_id = 1
        else:
            sort_tuple = (
                2,
                0 if has_frequency else 1,
                freq_value,
                due_sort,
                card_id,
            )
            bucket_id = 2

        if mode == "vocab_frequency":
            return (-vocab_count, *sort_tuple), bucket_id

        if mode == "frequency":
            if has_frequency:
                return (
                    0,
                    freq_value,
                    card_id,
                ), bucket_id
            return (
                1,
                due_sort,
                card_id,
            ), bucket_id

        # mode == "vocab"
        return sort_tuple, bucket_id

    def _apply_kanji_updates(
        self,
        collection: Collection,
        kanji_chars: Union[Sequence[str], Set[str]],
        dictionary: Dict[str, Dict[str, object]],
        kanji_model: NotetypeDict,
        kanji_field_indexes: Dict[str, int],
        kanji_field_index: int,
        cfg: AddonConfig,
        usage_info: Optional[Dict[str, KanjiUsageInfo]] = None,
        existing_notes: Optional[Dict[str, int]] = None,
        prune_existing: bool = False,
    ) -> Dict[str, object]:
        unique_chars: Set[str] = {char for char in kanji_chars if char}
        stats = {
            "kanji_scanned": len(unique_chars),
            "existing_tagged": 0,
            "created": 0,
            "unsuspended": 0,
            "missing_dictionary": set(),
            "tag_removed": 0,
            "resuspended": 0,
            "vocab_suspended": 0,
            "vocab_unsuspended": 0,
        }

        if not unique_chars:
            return stats

        if existing_notes is None:
            existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)

        unsuspend_tag = cfg.unsuspended_tag
        frequency_field_name: Optional[str] = None
        scheduling_field_name: Optional[str] = None
        fields_meta = kanji_model.get("flds") if isinstance(kanji_model, dict) else None
        freq_index = kanji_field_indexes.get("frequency")
        if isinstance(freq_index, int) and isinstance(fields_meta, list) and 0 <= freq_index < len(fields_meta):
            field_meta = fields_meta[freq_index]
            if isinstance(field_meta, dict):
                freq_name = field_meta.get("name")
                if isinstance(freq_name, str):
                    frequency_field_name = freq_name
        scheduling_index = kanji_field_indexes.get("scheduling_info")
        if isinstance(scheduling_index, int) and isinstance(fields_meta, list) and 0 <= scheduling_index < len(fields_meta):
            scheduling_meta = fields_meta[scheduling_index]
            if isinstance(scheduling_meta, dict):
                scheduling_name = scheduling_meta.get("name")
                if isinstance(scheduling_name, str):
                    scheduling_field_name = scheduling_name

        for kanji_char in unique_chars:
            dictionary_entry = dictionary.get(kanji_char)
            info = usage_info.get(kanji_char) if usage_info else None
            if kanji_char in existing_notes:
                note_id = existing_notes[kanji_char]
                tagged, note = self._ensure_note_tagged(collection, note_id, cfg.existing_tag)
                if tagged:
                    stats["existing_tagged"] += 1
                unsuspended = self._unsuspend_note_cards_if_needed(collection, note, unsuspend_tag)
                if unsuspended:
                    stats["unsuspended"] += unsuspended
                note_changed = False
                if frequency_field_name and isinstance(dictionary_entry, dict):
                    if self._update_frequency_field(note, frequency_field_name, dictionary_entry.get("frequency")):
                        note_changed = True
                if self._update_scheduling_info_field(
                    note,
                    scheduling_field_name,
                    kanji_char,
                    cfg,
                    dictionary_entry if isinstance(dictionary_entry, dict) else None,
                    info,
                ):
                    note_changed = True
                if info is not None:
                    self._update_kanji_status_tags(
                        note,
                        cfg,
                        has_vocab=True,
                        has_reviewed_vocab=info.reviewed,
                    )
                if note_changed:
                    note.flush()
                continue

            if dictionary_entry is None:
                stats["missing_dictionary"].add(kanji_char)
                continue

            created_note_id = self._create_kanji_note(
                collection,
                kanji_model,
                kanji_field_indexes,
                dictionary_entry,
                kanji_char,
                cfg.existing_tag,
                cfg.created_tag,
                cfg,
                info,
            )
            if created_note_id:
                stats["created"] += 1
                existing_notes[kanji_char] = created_note_id
                if info is not None:
                    new_note = _get_note(collection, created_note_id)
                    self._update_kanji_status_tags(
                        new_note,
                        cfg,
                        has_vocab=True,
                        has_reviewed_vocab=info.reviewed,
                    )

        if prune_existing and cfg.existing_tag:
            removed, resuspended = self._remove_unused_tags(
                collection,
                existing_notes,
                cfg.existing_tag,
                unsuspend_tag,
                unique_chars,
                cfg,
                frequency_field_name=frequency_field_name,
                dictionary=dictionary,
                scheduling_field_name=scheduling_field_name,
            )
            stats["tag_removed"] = removed
            stats["resuspended"] = resuspended

        return stats

    def _compute_kanji_interval_status(
        self,
        collection: Collection,
        existing_notes: Dict[str, int],
    ) -> Dict[str, KanjiIntervalStatus]:
        if not existing_notes:
            return {}
        note_ids = list(dict.fromkeys(existing_notes.values()))
        if not note_ids:
            return {}
        rows: List[Tuple[int, int, int]] = []
        for batch_index, batch_ids in enumerate(_chunk_sequence(note_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            rows.extend(
                _db_all(
                    collection,
                    (
                        "SELECT nid, "
                        "MAX(CASE WHEN type != 0 THEN 1 ELSE 0 END) AS reviewed_flag, "
                        "MAX(CASE WHEN type != 0 THEN ivl ELSE 0 END) AS max_interval "
                        f"FROM cards WHERE nid IN ({placeholders}) GROUP BY nid"
                    ),
                    *batch_ids,
                    context=f"compute_kanji_interval_status/batch{batch_index}",
                )
            )
        status_rows: Dict[int, Tuple[int, int]] = {}
        for nid, reviewed_flag, max_interval in rows:
            status_rows[nid] = (reviewed_flag, max_interval)

        historical_max_by_note: Dict[int, int] = {}
        for batch_index, batch_ids in enumerate(_chunk_sequence(note_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            historical_rows = _db_all(
                collection,
                (
                    "SELECT cards.nid, "
                    "MAX(CASE WHEN revlog.ivl > 0 THEN revlog.ivl ELSE 0 END) AS max_revlog_interval "
                    "FROM cards "
                    "JOIN revlog ON revlog.cid = cards.id "
                    f"WHERE cards.nid IN ({placeholders}) "
                    "GROUP BY cards.nid"
                ),
                *batch_ids,
                context=f"compute_kanji_interval_status/revlog_batch{batch_index}",
            )
            for nid, max_revlog_interval in historical_rows:
                try:
                    historical_max_by_note[nid] = int(max_revlog_interval)
                except Exception:
                    historical_max_by_note[nid] = 0

        status_by_note: Dict[int, KanjiIntervalStatus] = {}
        for note_id in note_ids:
            reviewed_flag, max_interval = status_rows.get(note_id, (0, 0))
            has_review_card = bool(reviewed_flag)
            try:
                current_interval = int(max_interval)
            except Exception:
                current_interval = 0
            if current_interval < 0:
                current_interval = 0
            historical_interval_raw = historical_max_by_note.get(note_id, 0)
            try:
                historical_interval = int(historical_interval_raw)
            except Exception:
                historical_interval = 0
            if historical_interval < 0:
                historical_interval = 0
            if historical_interval <= 0 or historical_interval < current_interval:
                historical_interval = current_interval
            status_by_note[note_id] = KanjiIntervalStatus(
                has_review_card=has_review_card,
                current_interval=current_interval,
                historical_interval=historical_interval,
            )

        return {
            char: status_by_note.get(note_id, KanjiIntervalStatus())
            for char, note_id in existing_notes.items()
        }

    def _fetch_vocab_rows(
        self,
        collection: Collection,
        model_id: int,
        target_chars: Optional[Set[str]] = None,
    ) -> List[Tuple[int, str, str]]:
        if not target_chars:
            return _db_all(
                collection,
                "SELECT id, flds, tags FROM notes WHERE mid = ?",
                model_id,
                context="fetch_vocab_rows/all",
            )
        chars = [char for char in target_chars if char]
        if not chars:
            return []
        clause = " OR ".join("instr(notes.flds, ?) > 0" for _ in chars)
        params: List[object] = [model_id, *chars]
        sql = f"SELECT id, flds, tags FROM notes WHERE mid = ? AND ({clause})"
        return _db_all(
            collection,
            sql,
            *params,
            context="fetch_vocab_rows/filter",
        )

    def _collect_vocab_note_chars(
        self,
        collection: Collection,
        vocab_field_map: Dict[int, List[int]],
        target_chars: Optional[Set[str]] = None,
    ) -> Dict[int, Tuple[Set[str], Set[str]]]:
        result: Dict[int, Tuple[Set[str], Set[str]]] = {}
        for model_id, field_indexes in vocab_field_map.items():
            if not field_indexes:
                continue
            rows = self._fetch_vocab_rows(collection, model_id, target_chars)
            if not rows:
                continue
            for note_id, flds, tags in rows:
                fields = flds.split("\x1f")
                chars: Set[str] = set()
                for field_index in field_indexes:
                    if field_index >= len(fields):
                        continue
                    chars.update(KANJI_PATTERN.findall(fields[field_index]))
                if not chars:
                    continue
                if target_chars and chars.isdisjoint(target_chars):
                    continue
                tag_set = {tag for tag in tags.strip().split() if tag}
                result[note_id] = (chars, tag_set)
        return result

    def _load_card_status_for_notes(
        self,
        collection: Collection,
        note_ids: Sequence[int],
    ) -> Dict[int, List[Tuple[int, int, int]]]:
        unique_ids = list(dict.fromkeys(note_ids))
        if not unique_ids:
            return {}
        rows: List[Tuple[int, int, int, int]] = []
        for batch_index, batch_ids in enumerate(_chunk_sequence(unique_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            rows.extend(
                _db_all(
                    collection,
                    f"SELECT id, nid, queue, type FROM cards WHERE nid IN ({placeholders})",
                    *batch_ids,
                    context=f"load_card_status_for_notes/batch{batch_index}",
                )
            )
        card_map: Dict[int, List[Tuple[int, int, int]]] = defaultdict(list)
        for card_id, nid, queue, ctype in rows:
            card_map[nid].append((card_id, queue, ctype))
        return card_map

    def _load_note_active_status(
        self,
        collection: Collection,
        note_ids: Sequence[int],
    ) -> Dict[int, bool]:
        unique_ids = list(dict.fromkeys(note_ids))
        if not unique_ids:
            return {}
        rows: List[Tuple[int, int]] = []
        for batch_index, batch_ids in enumerate(_chunk_sequence(unique_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            sql = (
                "SELECT nid, MAX(CASE WHEN queue != -1 THEN 1 ELSE 0 END) "
                f"FROM cards WHERE nid IN ({placeholders}) GROUP BY nid"
            )
            rows.extend(
                _db_all(
                    collection,
                    sql,
                    *batch_ids,
                    context=f"load_note_active_status/batch{batch_index}",
                )
            )
        return {nid: bool(flag) for nid, flag in rows}

    def _update_vocab_suspension(
        self,
        collection: Collection,
        cfg: AddonConfig,
        vocab_field_map: Dict[int, List[int]],
        existing_notes: Dict[str, int],
        target_chars: Optional[Set[str]] = None,
    ) -> Dict[str, int]:
        stats = {"vocab_suspended": 0, "vocab_unsuspended": 0}
        tag = cfg.auto_suspend_tag.strip()
        if not tag:
            self._debug("realtime/skip", reason="no_suspend_tag")
            return stats
        target_display = "".join(sorted(target_chars)) if target_chars else ""
        self._debug(
            "realtime/update_start",
            target=target_display,
        )
        notes_info = self._collect_vocab_note_chars(collection, vocab_field_map, target_chars)
        if not notes_info:
            self._debug("realtime/update_empty", target=target_display)
            return stats
        tag_lower = tag.lower()
        low_interval_tag = cfg.low_interval_vocab_tag.strip()
        low_interval_lower = low_interval_tag.lower()
        try:
            threshold = int(cfg.known_kanji_interval)
        except Exception:
            threshold = 0
        if threshold < 0:
            threshold = 0
        kanji_status = self._compute_kanji_interval_status(collection, existing_notes)
        card_map = self._load_card_status_for_notes(collection, notes_info.keys())

        for note_id, (chars, tag_set) in notes_info.items():
            tag_set_lower = {value.lower() for value in tag_set}
            note_has_tag = tag_lower in tag_set_lower
            cards = card_map.get(note_id, [])
            note_has_reviewed_card = any(
                isinstance(card_type, int) and card_type != 0
                for _, _, card_type in cards
            )
            requires_suspend = False
            for char in chars:
                status = kanji_status.get(char)
                if status is None:
                    requires_suspend = True
                    break
                if note_has_reviewed_card:
                    if cfg.resuspend_reviewed_low_interval:
                        if not status.has_review_card:
                            requires_suspend = True
                            break
                        if threshold > 0 and status.current_interval < threshold:
                            requires_suspend = True
                            break
                    else:
                        has_history = status.has_history
                        if not has_history:
                            requires_suspend = True
                            break
                        if threshold > 0 and status.historical_interval < threshold:
                            requires_suspend = True
                            break
                else:
                    if not status.has_review_card:
                        requires_suspend = True
                        break
                    if threshold > 0 and status.current_interval < threshold:
                        requires_suspend = True
                        break
            needs_low_interval_tag = False
            if low_interval_tag and threshold > 0:
                for char in chars:
                    status = kanji_status.get(char)
                    if not status:
                        continue
                    if status.has_review_card and status.current_interval < threshold:
                        needs_low_interval_tag = True
                        break
            note_obj: Optional[Note] = None
            changed = False

            def ensure_note() -> Note:
                nonlocal note_obj
                if note_obj is None:
                    note_obj = _get_note(collection, note_id)
                return note_obj

            if cfg.auto_suspend_vocab:
                if requires_suspend:
                    self._debug(
                        "realtime/keep_suspended",
                        note_id=note_id,
                        chars="".join(sorted(chars)),
                    )
                    unsuspended_cards = [card_id for card_id, queue, _ in cards if queue != -1]
                    newly_suspended = 0
                    if unsuspended_cards:
                        note_obj_local = ensure_note()
                        newly_suspended = _resuspend_note_cards(collection, note_obj_local)
                        if newly_suspended > 0:
                            stats["vocab_suspended"] += newly_suspended
                            existing_lower = {value.lower() for value in note_obj_local.tags}
                            if tag_lower not in existing_lower:
                                _add_tag(note_obj_local, tag)
                                changed = True
                                note_has_tag = True
                    else:
                        self._debug(
                            "realtime/already_suspended",
                            note_id=note_id,
                            chars="".join(sorted(chars)),
                        )
                else:
                    if note_has_tag:
                        note_obj_local = ensure_note()
                        suspended_cards = [card_id for card_id, queue, _ in cards if queue == -1]
                        if suspended_cards:
                            self._debug(
                                "realtime/unsuspend",
                                note_id=note_id,
                                chars="".join(sorted(chars)),
                                count=len(suspended_cards),
                            )
                            _unsuspend_cards(collection, suspended_cards)
                            stats["vocab_unsuspended"] += len(suspended_cards)
                        if _remove_tag_case_insensitive(note_obj_local, tag):
                            changed = True
                            note_has_tag = False
            else:
                if note_has_tag:
                    note_obj_local = ensure_note()
                    suspended_cards = [card_id for card_id, queue, _ in cards if queue == -1]
                    if suspended_cards:
                        _unsuspend_cards(collection, suspended_cards)
                        stats["vocab_unsuspended"] += len(suspended_cards)
                    if _remove_tag_case_insensitive(note_obj_local, tag):
                        changed = True
                        note_has_tag = False

            if low_interval_tag and threshold > 0:
                low_tag_present = False
                if note_obj is not None:
                    low_tag_present = any(value.lower() == low_interval_lower for value in note_obj.tags)
                else:
                    low_tag_present = low_interval_lower in tag_set_lower
                if needs_low_interval_tag:
                    if not low_tag_present:
                        note_obj_local = ensure_note()
                        _add_tag(note_obj_local, low_interval_tag)
                        changed = True
                elif low_tag_present:
                    note_obj_local = ensure_note()
                    if _remove_tag_case_insensitive(note_obj_local, low_interval_tag):
                        changed = True

            if note_obj is not None and changed:
                note_obj.flush()

        return stats

    def _create_kanji_note(
        self,
        collection: Collection,
        kanji_model: NotetypeDict,
        field_indexes: Dict[str, int],
        entry: Dict[str, object],
        kanji_char: str,
        existing_tag: str,
        created_tag: str,
        cfg: AddonConfig,
        usage: Optional[KanjiUsageInfo],
    ) -> Optional[int]:
        note = _new_note(collection, kanji_model)
        field_names = {logical: kanji_model["flds"][idx]["name"] for logical, idx in field_indexes.items()}

        self._assign_field(note, field_names.get("kanji"), kanji_char)
        self._assign_field(note, field_names.get("definition"), entry.get("definition", ""))
        stroke_count = entry.get("stroke_count", "")
        if isinstance(stroke_count, int):
            stroke_value = str(stroke_count)
        else:
            stroke_value = str(stroke_count or "")
        self._assign_field(note, field_names.get("stroke_count"), stroke_value)
        kunyomi_value = self._format_readings(entry.get("kunyomi"))
        self._assign_field(note, field_names.get("kunyomi"), kunyomi_value)
        onyomi_value = self._format_readings(entry.get("onyomi"))
        self._assign_field(note, field_names.get("onyomi"), onyomi_value)
        frequency_value = self._format_frequency_value(entry.get("frequency"))
        self._assign_field(note, field_names.get("frequency"), frequency_value)
        self._update_scheduling_info_field(
            note,
            field_names.get("scheduling_info"),
            kanji_char,
            cfg,
            entry,
            usage,
        )

        tags: List[str] = []
        if existing_tag:
            tags.append(existing_tag)
        if created_tag:
            tags.append(created_tag)
        for tag in tags:
            _add_tag(note, tag)

        deck_id = self._resolve_deck_id(collection, kanji_model, cfg)
        if not _add_note(collection, note, deck_id):
            return None
        return getattr(note, "id", None)

    def _assign_field(self, note: Note, field_name: Optional[str], value: str) -> None:
        if not field_name:
            return
        note[field_name] = value or ""

    def _format_frequency_value(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return str(int(value))
        if isinstance(value, str):
            return value.strip()
        return str(value)

    def _update_frequency_field(
        self,
        note: Note,
        field_name: Optional[str],
        value: object,
    ) -> bool:
        if not field_name:
            return False
        new_value = self._format_frequency_value(value)
        try:
            current = note[field_name]
        except KeyError:
            current = ""
        if current == new_value:
            return False
        note[field_name] = new_value
        return True

    def _build_scheduling_info_payload(
        self,
        *,
        kanji_char: str,
        cfg: AddonConfig,
        dictionary_entry: Optional[Dict[str, object]],
        usage: Optional[KanjiUsageInfo],
    ) -> str:
        def _render_optional(value: Optional[int]) -> str:
            if value is None:
                return "-"
            return str(value)

        def _render_bool(value: bool) -> str:
            return "yes" if value else "no"

        freq_value: Optional[object] = None
        if isinstance(dictionary_entry, dict):
            freq_value = dictionary_entry.get("frequency")
        frequency_text = self._format_frequency_value(freq_value)
        frequency_display = frequency_text if frequency_text else "-"

        mode = cfg.reorder_mode or "vocab"
        has_usage = usage is not None
        reviewed = bool(getattr(usage, "reviewed", False)) if usage else False
        vocab_occurrences: Optional[int] = getattr(usage, "vocab_occurrences", None) if usage else None

        lines = [
            f"kanji: {kanji_char or '-'}",
            f"reorder_mode: {mode}",
            f"has_vocab_usage: {_render_bool(has_usage)}",
            f"reviewed_vocab: {_render_bool(reviewed)}",
            f"first_review_order: {_render_optional(getattr(usage, 'first_review_order', None) if usage else None)}",
            f"first_review_due: {_render_optional(getattr(usage, 'first_review_due', None) if usage else None)}",
            f"first_new_due: {_render_optional(getattr(usage, 'first_new_due', None) if usage else None)}",
            f"first_new_order: {_render_optional(getattr(usage, 'first_new_order', None) if usage else None)}",
            f"vocab_occurrences: {_render_optional(vocab_occurrences)}",
            f"dictionary_entry: {_render_bool(isinstance(dictionary_entry, dict))}",
            f"dictionary_frequency: {frequency_display}",
        ]
        return "\n".join(lines)

    def _update_scheduling_info_field(
        self,
        note: Note,
        field_name: Optional[str],
        kanji_char: str,
        cfg: AddonConfig,
        dictionary_entry: Optional[Dict[str, object]],
        usage: Optional[KanjiUsageInfo],
    ) -> bool:
        if not cfg.store_scheduling_info:
            return False
        if not field_name:
            return False
        new_value = self._build_scheduling_info_payload(
            kanji_char=kanji_char,
            cfg=cfg,
            dictionary_entry=dictionary_entry,
            usage=usage,
        )
        try:
            current = note[field_name]
        except KeyError:
            current = ""
        if current == new_value:
            return False
        note[field_name] = new_value
        return True

    def _format_readings(self, value: object) -> str:
        if isinstance(value, list):
            return "; ".join(str(item) for item in value if item)
        return str(value or "")

    def _unsuspend_note_cards_if_needed(
        self,
        collection: Collection,
        note: Note,
        unsuspend_tag: str,
    ) -> int:
        leech_tag = "leech"
        try:
            col_conf = collection.conf
        except AttributeError:
            col_conf = {}
        if isinstance(col_conf, dict):
            leech_tag = (col_conf.get("leechTag") or leech_tag).strip() or "leech"

        leech_lower = leech_tag.lower()
        note_tags_lower = {tag.lower() for tag in note.tags}
        if leech_lower and leech_lower in note_tags_lower:
            return 0

        card_rows = _db_all(
            collection,
            "SELECT id, queue FROM cards WHERE nid = ?",
            note.id,
            context="unsuspend_note_cards_if_needed",
        )
        to_unsuspend = [card_id for card_id, queue in card_rows if queue == -1]
        if not to_unsuspend:
            return 0

        _unsuspend_cards(collection, to_unsuspend)

        changed = False
        if unsuspend_tag and unsuspend_tag not in note.tags:
            _add_tag(note, unsuspend_tag)
            changed = True
        if changed:
            note.flush()
        return len(to_unsuspend)

    def _resolve_deck_id(self, collection: Collection, model: NotetypeDict, cfg: AddonConfig) -> int:
        if cfg.kanji_deck_name:
            deck_id = self._lookup_deck_id(collection, cfg.kanji_deck_name)
            if deck_id:
                self._missing_deck_logged = False
                return deck_id
            if not self._missing_deck_logged:
                _safe_print(
                    "[KanjiCards] Configured kanji deck '%s' was not found; using fallback"
                    % cfg.kanji_deck_name
                )
                self._missing_deck_logged = True

        did = model.get("did")
        if isinstance(did, int) and did > 0:
            return did

        decks = collection.decks
        for attr in ("get_current_id", "current_id", "selected"):
            getter = getattr(decks, attr, None)
            if callable(getter):
                try:
                    deck_id = getter()
                    if isinstance(deck_id, int) and deck_id > 0:
                        return deck_id
                except TypeError:
                    continue

        current = getattr(decks, "current", None)
        if callable(current):
            try:
                deck = current()
            except TypeError:
                deck = None
        else:
            deck = current

        if isinstance(deck, dict):
            deck_id = deck.get("id")
            if isinstance(deck_id, int) and deck_id > 0:
                return deck_id

        get = getattr(decks, "id", None)
        if callable(get):
            try:
                return get("Default")
            except Exception:
                pass

        # Fallback to the first deck available
        all_decks = getattr(decks, "all_names_and_ids", None)
        if callable(all_decks):
            entries = all_decks()
            if entries:
                return entries[0].id if hasattr(entries[0], "id") else entries[0][1]

        raise RuntimeError("Unable to determine a deck for new kanji notes")

    def _lookup_deck_id(self, collection: Collection, name: str) -> Optional[int]:
        decks = collection.decks
        if not name:
            return None
        for attr in ("id_for_name", "idForName", "id"):
            getter = getattr(decks, attr, None)
            if callable(getter):
                try:
                    deck_id = getter(name)
                    if isinstance(deck_id, int) and deck_id > 0:
                        return deck_id
                except Exception:
                    continue
        # Fall back to scanning deck list
        fetcher = getattr(decks, "all_names_and_ids", None)
        if callable(fetcher):
            try:
                entries = fetcher()
            except TypeError:
                entries = None
            if entries:
                for entry in entries:
                    entry_name = self._deck_entry_name(entry)
                    entry_id = getattr(entry, "id", None)
                    if entry_id is None and isinstance(entry, (list, tuple)):
                        entry_id = next((item for item in entry if isinstance(item, int)), None)
                    if entry_name == name and isinstance(entry_id, int) and entry_id > 0:
                        return entry_id
        return None

    def _deck_entry_name(self, entry: Any) -> Optional[str]:
        if entry is None:
            return None
        if hasattr(entry, "name"):
            candidate = entry.name
        elif isinstance(entry, (list, tuple)):
            candidate = next((item for item in entry if isinstance(item, str)), None)
        elif isinstance(entry, str):
            candidate = entry
        else:
            candidate = None
        if not candidate:
            return None
        return str(candidate)


class KanjiVocabRecalcSettingsDialog(QDialog):  # pragma: no cover
    """Settings dialog for configuring the add-on."""

    def __init__(self, manager: KanjiVocabRecalcManager, config: AddonConfig) -> None:  # pragma: no cover
        super().__init__(manager.mw)
        self.setWindowTitle("KanjiCards Settings")
        self.manager = manager
        self.config = config

        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._build_general_tab()
        self._build_kanji_tab()
        self._build_vocab_tab()

        buttons = QDialogButtonBox(BUTTON_OK | BUTTON_CANCEL)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    # ------------------------------------------------------------------
    # Tabs
    # ------------------------------------------------------------------
    def _build_general_tab(self) -> None:  # pragma: no cover
        widget = QGroupBox("General")
        form = QFormLayout(widget)

        self.existing_tag_edit = QLineEdit(self.config.existing_tag)
        self.created_tag_edit = QLineEdit(self.config.created_tag)
        self.dictionary_edit = QLineEdit(self.config.dictionary_file)
        self.unsuspend_tag_edit = QLineEdit(self.config.unsuspended_tag)
        self.deck_combo = QComboBox()
        self._populate_deck_combo()
        self.realtime_check = QCheckBox("Update during reviews")
        self.realtime_check.setChecked(self.config.realtime_review)
        self.known_interval_spin = QSpinBox()
        self.known_interval_spin.setRange(0, 3650)
        try:
            interval_value = int(self.config.known_kanji_interval)
        except Exception:
            interval_value = 0
        self.known_interval_spin.setValue(max(0, interval_value))
        self.auto_sync_check = QCheckBox("Run automatically after sync")
        self.auto_sync_check.setChecked(self.config.auto_run_on_sync)
        self.ignore_suspended_check = QCheckBox("Ignore suspended vocab cards")
        self.ignore_suspended_check.setChecked(self.config.ignore_suspended_vocab)
        self.auto_suspend_check = QCheckBox("Suspend vocab with unreviewed kanji")
        self.auto_suspend_check.setChecked(self.config.auto_suspend_vocab)
        self.auto_suspend_tag_edit = QLineEdit(self.config.auto_suspend_tag)
        self.auto_suspend_tag_edit.setEnabled(self.config.auto_suspend_vocab)
        self.auto_suspend_check.toggled.connect(self.auto_suspend_tag_edit.setEnabled)
        self.resuspend_reviewed_check = QCheckBox("Resuspend reviewed vocab when current interval drops below threshold")
        self.resuspend_reviewed_check.setChecked(self.config.resuspend_reviewed_low_interval)
        self.low_interval_vocab_tag_edit = QLineEdit(self.config.low_interval_vocab_tag)
        self.reorder_combo = QComboBox()
        self.reorder_combo.addItem("Frequency (KANJIDIC)", "frequency")
        self.reorder_combo.addItem("Vocabulary frequency", "vocab_frequency")
        self.reorder_combo.addItem("Vocabulary order", "vocab")
        current_mode = (
            self.config.reorder_mode
            if self.config.reorder_mode in {"frequency", "vocab", "vocab_frequency"}
            else "vocab"
        )
        index = self.reorder_combo.findData(current_mode)
        if index >= 0:
            self.reorder_combo.setCurrentIndex(index)
        self.bucket_reviewed_tag_edit = QLineEdit(self.config.bucket_tags.get("reviewed_vocab", ""))
        self.bucket_unreviewed_tag_edit = QLineEdit(self.config.bucket_tags.get("unreviewed_vocab", ""))
        self.bucket_no_vocab_tag_edit = QLineEdit(self.config.bucket_tags.get("no_vocab", ""))
        self.only_new_vocab_tag_edit = QLineEdit(self.config.only_new_vocab_tag)
        self.no_vocab_tag_edit = QLineEdit(self.config.no_vocab_tag)

        form.addRow("Existing kanji tag", self.existing_tag_edit)
        form.addRow("Auto-created kanji tag", self.created_tag_edit)
        form.addRow("Dictionary file", self.dictionary_edit)
        form.addRow("Unsuspended tag", self.unsuspend_tag_edit)
        form.addRow("Kanji deck", self.deck_combo)
        form.addRow("", self.realtime_check)
        form.addRow("Known kanji interval (days)", self.known_interval_spin)
        form.addRow("", self.auto_sync_check)
        form.addRow("", self.ignore_suspended_check)
        form.addRow("", self.auto_suspend_check)
        form.addRow("Suspension tag", self.auto_suspend_tag_edit)
        form.addRow("", self.resuspend_reviewed_check)
        form.addRow("Below-threshold vocab tag", self.low_interval_vocab_tag_edit)
        form.addRow("Order new kanji cards", self.reorder_combo)
        form.addRow("Reviewed vocab bucket tag", self.bucket_reviewed_tag_edit)
        form.addRow("Unreviewed vocab bucket tag", self.bucket_unreviewed_tag_edit)
        form.addRow("No vocab bucket tag", self.bucket_no_vocab_tag_edit)
        form.addRow("Only-new vocab kanji tag", self.only_new_vocab_tag_edit)
        form.addRow("No-vocab kanji tag", self.no_vocab_tag_edit)

        self.tabs.addTab(widget, "General")

    def _build_kanji_tab(self) -> None:  # pragma: no cover
        widget = QWidget()
        layout = QFormLayout(widget)

        self.kanji_model_combo = QComboBox()
        self.models_by_index: List[Optional[NotetypeDict]] = []
        self._populate_model_combo(self.kanji_model_combo, self.models_by_index, self.config.kanji_note_type.name)

        layout.addRow("Kanji note type", self.kanji_model_combo)

        self.kanji_field_combos: Dict[str, QComboBox] = {}
        for logical_field, label in [
            ("kanji", "Kanji field"),
            ("definition", "Definition field"),
            ("stroke_count", "Stroke count field"),
            ("kunyomi", "Kunyomi field"),
            ("onyomi", "Onyomi field"),
            ("frequency", "Frequency field"),
            ("scheduling_info", "Scheduling info field"),
        ]:
            combo = QComboBox()
            self.kanji_field_combos[logical_field] = combo
            layout.addRow(label, combo)

        self.scheduling_info_check = QCheckBox("Store scheduling info on kanji notes")
        self.scheduling_info_check.setChecked(bool(self.config.store_scheduling_info))
        self.scheduling_info_check.toggled.connect(lambda _checked: self._refresh_kanji_field_combos())
        layout.addRow(self.scheduling_info_check)

        self.kanji_model_combo.currentIndexChanged.connect(self._refresh_kanji_field_combos)
        self._refresh_kanji_field_combos()

        self.tabs.addTab(widget, "Kanji note")

    def _build_vocab_tab(self) -> None:  # pragma: no cover
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self.vocab_list = QListWidget()
        self.vocab_list.setSelectionMode(SINGLE_SELECTION)
        layout.addWidget(self.vocab_list)

        button_row = QHBoxLayout()
        add_button = QPushButton("Add")
        edit_button = QPushButton("Edit")
        remove_button = QPushButton("Remove")
        button_row.addWidget(add_button)
        button_row.addWidget(edit_button)
        button_row.addWidget(remove_button)
        layout.addLayout(button_row)

        add_button.clicked.connect(self._add_vocab_entry)
        edit_button.clicked.connect(self._edit_vocab_entry)
        remove_button.clicked.connect(self._remove_vocab_entry)

        self.tabs.addTab(widget, "Vocab notes")

        self._reload_vocab_entries()

    # ------------------------------------------------------------------
    # Dialog helpers
    # ------------------------------------------------------------------
    def _populate_model_combo(
        self,
        combo: QComboBox,
        container: List[Optional[NotetypeDict]],
        selected_name: str,
    ) -> None:  # pragma: no cover
        combo.clear()
        container.clear()
        models = self.manager.mw.col.models.all()
        names = sorted(model["name"] for model in models)
        combo.addItem("<Select note type>")
        container.append(None)
        selected_index = 0
        for idx, name in enumerate(names, start=1):
            combo.addItem(name)
            container.append(self.manager.mw.col.models.byName(name))
            if name == selected_name:
                selected_index = idx
        combo.setCurrentIndex(selected_index)

    def _refresh_kanji_field_combos(self) -> None:  # pragma: no cover
        model = self._current_kanji_model()
        scheduling_enabled = bool(getattr(self, "scheduling_info_check", None) and self.scheduling_info_check.isChecked())
        for logical, combo in self.kanji_field_combos.items():
            combo.clear()
            combo.addItem("<Not set>")
            if not model:
                combo.setEnabled(logical != "scheduling_info")
                continue
            raw_fields = model.get("flds", [])
            field_names = [
                fld.get("name")
                for fld in raw_fields
                if isinstance(fld, dict) and isinstance(fld.get("name"), str)
            ]
            combo.addItems(field_names)
            current_name = self.config.kanji_note_type.fields.get(logical, "")
            if logical == "scheduling_info" and scheduling_enabled and not current_name:
                if SCHEDULING_FIELD_DEFAULT_NAME in field_names:
                    current_name = SCHEDULING_FIELD_DEFAULT_NAME
                    self.config.kanji_note_type.fields[logical] = current_name
            try:
                if current_name and current_name in field_names:
                    combo.setCurrentIndex(field_names.index(current_name) + 1)
                else:
                    combo.setCurrentIndex(0)
            except ValueError:
                combo.setCurrentIndex(0)
            combo.setEnabled(logical != "scheduling_info" or scheduling_enabled)

    def _current_kanji_model(self) -> Optional[NotetypeDict]:  # pragma: no cover
        index = self.kanji_model_combo.currentIndex()
        if index < 0 or index >= len(self.models_by_index):
            return None
        return self.models_by_index[index]

    def _suggest_scheduling_field_name(self, model: NotetypeDict) -> str:  # pragma: no cover
        existing = {
            fld.get("name")
            for fld in model.get("flds", [])
            if isinstance(fld, dict) and isinstance(fld.get("name"), str)
        }
        base = SCHEDULING_FIELD_DEFAULT_NAME
        if base not in existing:
            return base
        suffix = 2
        while True:
            candidate = f"{base} {suffix}"
            if candidate not in existing:
                return candidate
            suffix += 1

    def _confirm_scheduling_field_full_sync(self) -> bool:  # pragma: no cover
        message = (
            "Adding the scheduling info field will force a full sync.\n"
            "Make sure all of your other devices are synced before continuing."
        )
        if QMessageBox is not None:
            try:
                ok_button = QMessageBox.StandardButton.Ok
                cancel_button = QMessageBox.StandardButton.Cancel
            except AttributeError:
                ok_button = QMessageBox.Ok
                cancel_button = QMessageBox.Cancel
            result = QMessageBox.question(
                self,
                "Full Sync Required",
                message,
                ok_button | cancel_button,
                cancel_button,
            )
            return result == ok_button
        if askUser is not None:
            try:
                return bool(askUser(message, defaultno=True))
            except TypeError:
                return bool(askUser(message))
        show_warning("Cannot confirm scheduling field addition without user input; cancelling.")
        return False

    def _create_scheduling_field(self, model: NotetypeDict, field_name: str) -> bool:  # pragma: no cover
        mw_obj = getattr(self.manager, "mw", None)
        col = getattr(mw_obj, "col", None) if mw_obj else None
        if not col:
            show_warning("Collection is not available; cannot create the scheduling info field.")
            return False
        models = getattr(col, "models", None)
        if not models:
            show_warning("Note type manager unavailable; cannot create the scheduling info field.")
            return False
        try:
            new_field = models.newField(field_name)
            models.addField(model, new_field)
            models.save(model)
        except Exception as err:  # noqa: BLE001
            show_warning(f"Unable to create the scheduling info field:\n{err}")
            return False
        self.manager._kanji_model_cache = None
        self.manager._existing_notes_cache = None
        reset = getattr(mw_obj, "reset", None)
        if callable(reset):
            try:
                reset()
            except Exception:
                pass
        return True

    def _reload_vocab_entries(self) -> None:  # pragma: no cover
        self.vocab_list.clear()
        for entry in self.config.vocab_note_types:
            fields = ", ".join(entry.fields)
            multiplier = float(entry.due_multiplier) if entry.due_multiplier else 1.0
            if multiplier <= 0:
                multiplier = 1.0
            multiplier_text = f"×{multiplier:g}"
            suffix = f" ({multiplier_text})" if multiplier != 1.0 else ""
            item = QListWidgetItem(f"{entry.name} — {fields}{suffix}")
            item.setData(USER_ROLE, entry)
            self.vocab_list.addItem(item)

    def _add_vocab_entry(self) -> None:  # pragma: no cover
        dialog = VocabNoteConfigDialog(self.manager)
        if dialog.exec() == DIALOG_ACCEPTED:
            cfg = dialog.get_result()
            self.config.vocab_note_types.append(cfg)
            self._reload_vocab_entries()

    def _edit_vocab_entry(self) -> None:  # pragma: no cover
        current_item = self.vocab_list.currentItem()
        if not current_item:
            return
        existing_cfg: VocabNoteTypeConfig = current_item.data(USER_ROLE)
        dialog = VocabNoteConfigDialog(self.manager, existing_cfg)
        if dialog.exec() == DIALOG_ACCEPTED:
            new_cfg = dialog.get_result()
            index = self.config.vocab_note_types.index(existing_cfg)
            self.config.vocab_note_types[index] = new_cfg
            self._reload_vocab_entries()

    def _remove_vocab_entry(self) -> None:  # pragma: no cover
        current_item = self.vocab_list.currentItem()
        if not current_item:
            return
        existing_cfg: VocabNoteTypeConfig = current_item.data(USER_ROLE)
        self.config.vocab_note_types.remove(existing_cfg)
        self._reload_vocab_entries()

    # ------------------------------------------------------------------
    # Validation and persistence
    # ------------------------------------------------------------------
    def accept(self) -> None:  # noqa: D401
        if not self._persist_general_tab():
            return
        if not self._persist_kanji_tab():
            return
        if not self._validate_vocab_entries():
            return
        self.manager.save_config(self.config)
        super().accept()

    def _persist_general_tab(self) -> bool:
        self.config.existing_tag = self.existing_tag_edit.text().strip()
        self.config.created_tag = self.created_tag_edit.text().strip()
        self.config.dictionary_file = self.dictionary_edit.text().strip()
        if not self.config.dictionary_file:
            show_warning("Please provide a dictionary file path.")
            return False
        deck_name = self.deck_combo.currentData()
        self.config.kanji_deck_name = deck_name.strip() if isinstance(deck_name, str) else ""
        self.config.unsuspended_tag = self.unsuspend_tag_edit.text().strip()
        self.config.realtime_review = self.realtime_check.isChecked()
        try:
            known_interval = int(self.known_interval_spin.value())
        except Exception:
            known_interval = 0
        self.config.known_kanji_interval = max(0, known_interval)
        self.config.auto_run_on_sync = self.auto_sync_check.isChecked()
        self.config.ignore_suspended_vocab = self.ignore_suspended_check.isChecked()
        auto_suspend_enabled = self.auto_suspend_check.isChecked()
        auto_suspend_tag = self.auto_suspend_tag_edit.text().strip()
        if auto_suspend_enabled and not auto_suspend_tag:
            show_warning("Provide a suspension tag when auto-suspend is enabled.")
            return False
        self.config.auto_suspend_vocab = auto_suspend_enabled
        self.config.auto_suspend_tag = auto_suspend_tag
        self.config.resuspend_reviewed_low_interval = self.resuspend_reviewed_check.isChecked()
        self.config.reorder_mode = self.reorder_combo.currentData() or "vocab"
        self.config.bucket_tags = {
            "reviewed_vocab": self.bucket_reviewed_tag_edit.text().strip(),
            "unreviewed_vocab": self.bucket_unreviewed_tag_edit.text().strip(),
            "no_vocab": self.bucket_no_vocab_tag_edit.text().strip(),
        }
        self.config.only_new_vocab_tag = self.only_new_vocab_tag_edit.text().strip()
        self.config.no_vocab_tag = self.no_vocab_tag_edit.text().strip()
        self.config.low_interval_vocab_tag = self.low_interval_vocab_tag_edit.text().strip()
        return True

    def _populate_deck_combo(self) -> None:
        self.deck_combo.clear()
        self.deck_combo.addItem("<Use note type/default deck>", "")

        col = self.manager.mw.col
        if not col:
            return
        decks = col.decks
        deck_names: List[str] = []

        entries = None
        for attr in ("all_names_and_ids", "allNamesAndIds", "allNames"):
            getter = getattr(decks, attr, None)
            if callable(getter):
                try:
                    entries = getter()
                    break
                except TypeError:
                    continue
        if entries is None:
            entries = []

        iterable = entries if isinstance(entries, list) else list(entries or [])
        for entry in iterable:
            name = self.manager._deck_entry_name(entry)
            if name:
                deck_names.append(name)

        for name in sorted(set(deck_names)):
            self.deck_combo.addItem(name, name)

        current_name = self.config.kanji_deck_name
        if current_name:
            index = self.deck_combo.findData(current_name)
            if index != -1:
                self.deck_combo.setCurrentIndex(index)

    def _persist_kanji_tab(self) -> bool:
        model = self._current_kanji_model()
        if not model:
            show_warning("Please choose a kanji note type.")
            return False
        self.config.kanji_note_type.name = model["name"]
        for logical, combo in self.kanji_field_combos.items():
            if combo.currentIndex() <= 0:
                self.config.kanji_note_type.fields[logical] = ""
                continue
            self.config.kanji_note_type.fields[logical] = combo.currentText()
        field_names = {
            fld.get("name")
            for fld in model.get("flds", [])
            if isinstance(fld, dict) and isinstance(fld.get("name"), str)
        }
        self.config.store_scheduling_info = bool(self.scheduling_info_check.isChecked())
        if not self.config.store_scheduling_info:
            self.config.kanji_note_type.fields["scheduling_info"] = ""
        else:
            scheduling_field = self.config.kanji_note_type.fields.get("scheduling_info", "").strip()
            if not scheduling_field:
                if SCHEDULING_FIELD_DEFAULT_NAME in field_names:
                    scheduling_field = SCHEDULING_FIELD_DEFAULT_NAME
                else:
                    scheduling_field = self._suggest_scheduling_field_name(model)
                self.config.kanji_note_type.fields["scheduling_info"] = scheduling_field
            if scheduling_field not in field_names:
                if not self._confirm_scheduling_field_full_sync():
                    return False
                if not self._create_scheduling_field(model, scheduling_field):
                    return False
                field_names = {
                    fld.get("name")
                    for fld in model.get("flds", [])
                    if isinstance(fld, dict) and isinstance(fld.get("name"), str)
                }
                if scheduling_field not in field_names:
                    show_warning("Failed to add the scheduling info field to the kanji note type.")
                    return False
                self._refresh_kanji_field_combos()
        required_field = self.config.kanji_note_type.fields.get("kanji", "").strip()
        if not required_field:
            show_warning("Please assign a field that stores the kanji character.")
            return False
        return True

    def _validate_vocab_entries(self) -> bool:
        if not self.config.vocab_note_types:
            show_warning("Configure at least one vocabulary note type to scan.")
            return False
        return True


class VocabNoteConfigDialog(QDialog):  # pragma: no cover
    """Dialog to configure a single vocabulary note type entry."""

    def __init__(
        self,
        manager: KanjiVocabRecalcManager,
        existing: Optional[VocabNoteTypeConfig] = None,
    ) -> None:  # pragma: no cover
        super().__init__(manager.mw)
        self.manager = manager
        self.setWindowTitle("Vocabulary Note Type")
        self.existing = existing

        layout = QVBoxLayout(self)

        form = QFormLayout()
        layout.addLayout(form)

        self.model_combo = QComboBox()
        self._models: List[Optional[NotetypeDict]] = []
        selected_name = existing.name if existing else ""
        self._populate_models(selected_name)
        form.addRow("Note type", self.model_combo)

        self.multiplier_spin = QSpinBox()
        self.multiplier_spin.setRange(1, 100000)
        initial_multiplier = getattr(existing, "due_multiplier", 1.0) if existing else 1.0
        try:
            initial_multiplier = float(initial_multiplier)
        except Exception:
            initial_multiplier = 1.0
        if initial_multiplier <= 0:
            initial_multiplier = 1.0
        initial_int = int(round(initial_multiplier)) if initial_multiplier else 1
        if initial_int <= 0:
            initial_int = 1
        if initial_int > 100000:
            initial_int = 100000
        self.multiplier_spin.setValue(initial_int)
        form.addRow("Due multiplier", self.multiplier_spin)

        self.fields_list = QListWidget()
        self.fields_list.setSelectionMode(NO_SELECTION)
        layout.addWidget(QLabel("Fields to scan for kanji"))
        layout.addWidget(self.fields_list)
        self.model_combo.currentIndexChanged.connect(self._populate_fields)
        self._populate_fields()

        buttons = QDialogButtonBox(BUTTON_OK | BUTTON_CANCEL)
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _populate_models(self, selected_name: str) -> None:  # pragma: no cover
        models = self.manager.mw.col.models.all()
        names = sorted(model["name"] for model in models)
        self.model_combo.addItem("<Select note type>")
        self._models.append(None)
        selected_index = 0
        for idx, name in enumerate(names, start=1):
            self.model_combo.addItem(name)
            self._models.append(self.manager.mw.col.models.byName(name))
            if name == selected_name:
                selected_index = idx
        self.model_combo.setCurrentIndex(selected_index)

    def _populate_fields(self) -> None:  # pragma: no cover
        self.fields_list.clear()
        model = self._current_model()
        if not model:
            return
        existing_fields = set(self.existing.fields) if self.existing else set()
        for field in model["flds"]:
            item = QListWidgetItem(field["name"])
            item.setFlags(item.flags() | ITEM_IS_USER_CHECKABLE)
            item.setCheckState(CHECKED_STATE if field["name"] in existing_fields else UNCHECKED_STATE)
            self.fields_list.addItem(item)

    def _current_model(self) -> Optional[NotetypeDict]:  # pragma: no cover
        index = self.model_combo.currentIndex()
        if index < 0 or index >= len(self._models):
            return None
        return self._models[index]

    def _on_accept(self) -> None:  # pragma: no cover
        model = self._current_model()
        if not model:
            show_warning("Please choose a note type.")
            return
        selected_fields = [
            self.fields_list.item(index).text()
            for index in range(self.fields_list.count())
            if self.fields_list.item(index).checkState() == CHECKED_STATE
        ]
        if not selected_fields:
            show_warning("Select at least one field to scan for kanji.")
            return
        multiplier_value = float(self.multiplier_spin.value())
        if multiplier_value <= 0:
            multiplier_value = 1.0
        self.existing = VocabNoteTypeConfig(
            name=model["name"],
            fields=selected_fields,
            due_multiplier=float(multiplier_value),
        )
        self.accept()

    def get_result(self) -> VocabNoteTypeConfig:  # pragma: no cover
        if not self.existing:
            raise RuntimeError("Dialog accepted without configuration")
        return self.existing


# Backwards-compatible aliases for external integrations
KanjiVocabSyncManager = KanjiVocabRecalcManager
KanjiVocabSyncSettingsDialog = KanjiVocabRecalcSettingsDialog


_manager: Optional[KanjiVocabRecalcManager] = None


def _initialize_manager() -> None:
    global _manager
    if _manager is None and mw is not None:
        _manager = KanjiVocabRecalcManager()


def on_profile_loaded() -> None:
    _initialize_manager()


def on_main_window_did_init() -> None:
    _initialize_manager()


gui_hooks.profile_did_open.append(on_profile_loaded)
gui_hooks.main_window_did_init.append(on_main_window_did_init)


# ----------------------------------------------------------------------
# Backwards compatibility helpers
# ----------------------------------------------------------------------
def _add_note(collection: Collection, note: Note, deck_id: Optional[int] = None) -> bool:
    handler = getattr(collection, "add_note", None)
    if callable(handler):
        try:
            if deck_id is None:
                return handler(note)
            return handler(note, deck_id)
        except TypeError as err:
            message = str(err)
            if "deck_id" in message:
                if deck_id is None:
                    raise
                return handler(note, deck_id)
            raise
    return collection.addNote(note)


def _unsuspend_cards(collection: Collection, card_ids: Sequence[int]) -> None:
    if not card_ids:
        return
    sched = getattr(collection, "sched", None)
    if sched is not None:
        for attr in ("unsuspend_cards", "unsuspendCards"):
            func = getattr(sched, attr, None)
            if callable(func):
                func(list(card_ids))
                return

    placeholders = ",".join("?" for _ in card_ids)
    params: List[object] = [intTime(), collection.usn(), *card_ids]
    _db_execute(
        collection,
        f"UPDATE cards SET mod = ?, usn = ?, queue = type WHERE id IN ({placeholders})",
        *params,
        context="unsuspend_cards",
    )


def _resuspend_note_cards(collection: Collection, note: Note) -> int:
    card_rows = _db_all(
        collection,
        "SELECT id, queue FROM cards WHERE nid = ?",
        note.id,
        context="resuspend_note_cards/load",
    )
    to_suspend = [card_id for card_id, queue in card_rows if queue != -1]
    if not to_suspend:
        return 0

    sched = getattr(collection, "sched", None)
    if sched is not None:
        for attr in ("suspend_cards", "suspendCards"):
            func = getattr(sched, attr, None)
            if callable(func):
                func(list(to_suspend))
                return len(to_suspend)

    placeholders = ",".join("?" for _ in to_suspend)
    params: List[object] = [intTime(), collection.usn(), *to_suspend]
    _db_execute(
        collection,
        f"UPDATE cards SET mod = ?, usn = ?, queue = -1 WHERE id IN ({placeholders})",
        *params,
        context="resuspend_note_cards/update",
    )
    return len(to_suspend)


def _db_all(
    collection: Collection,
    sql: str,
    *params: object,
    context: str = "",
) -> List[Tuple]:
    try:
        return collection.db.all(sql, *params)
    except Exception as err:  # noqa: BLE001
        _log_db_error("all", sql, params, context, err)
        raise


def _db_execute(
    collection: Collection,
    sql: str,
    *params: object,
    context: str = "",
) -> None:
    try:
        collection.db.execute(sql, *params)
    except Exception as err:  # noqa: BLE001
        _log_db_error("execute", sql, params, context, err)
        raise


def _log_db_error(
    operation: str,
    sql: str,
    params: Sequence[object],
    context: str,
    err: Exception,
) -> None:
    prefix = "[KanjiCards] db.%s failed" % operation
    if context:
        prefix += f" ({context})"
    _safe_print(prefix + f": {err}")
    _safe_print(f"  SQL: {sql}")
    if params:
        _safe_print(f"  Params: {params}")


def _chunk_sequence(values: Sequence[int], chunk_size: int) -> Iterator[List[int]]:
    """Yield slices limited by SQLite parameter cap."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for start in range(0, len(values), chunk_size):
        yield list(values[start : start + chunk_size])


def _new_note(collection: Collection, model: NotetypeDict) -> Note:
    handler = getattr(collection, "new_note", None)
    if callable(handler):
        return handler(model)
    return collection.newNote(model)


def _get_note(collection: Collection, note_id: int) -> Note:
    handler = getattr(collection, "get_note", None)
    if callable(handler):
        return handler(note_id)
    return collection.getNote(note_id)


def _add_tag(note: Note, tag: str) -> None:
    handler = getattr(note, "add_tag", None)
    if callable(handler):
        handler(tag)
        return
    note.addTag(tag)


def _remove_tag(note: Note, tag: str) -> None:
    handler = getattr(note, "remove_tag", None)
    if callable(handler):
        handler(tag)
        return
    note.removeTag(tag)


def _remove_tag_case_insensitive(note: Note, tag: str) -> bool:
    cleaned = tag.strip()
    if not cleaned:
        return False
    target = cleaned.lower()
    removed = False
    for existing in list(note.tags):
        if existing.lower() == target:
            _remove_tag(note, existing)
            removed = True
    return removed
