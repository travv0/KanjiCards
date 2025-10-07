"""KanjiCards add-on.

This add-on inspects configured vocabulary notes that the user has reviewed
and ensures each kanji found in those notes has a corresponding kanji card.
Existing kanji cards receive a configurable tag, and missing ones are created
automatically using dictionary data and tagged accordingly.
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union

from anki.collection import Collection
from anki.models import NotetypeDict
from anki.notes import Note
from anki.utils import intTime
from aqt import gui_hooks, mw
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
except ImportError:  # pragma: no cover
    from aqt.utils import showCritical as show_critical  # pragma: no cover
    from aqt.utils import showInfo as show_info  # pragma: no cover
    from aqt.utils import showWarning as show_warning  # pragma: no cover
    from aqt.utils import tooltip  # pragma: no cover

try:  # PyQt6-style enums
    SINGLE_SELECTION = QAbstractItemView.SelectionMode.SingleSelection
    NO_SELECTION = QAbstractItemView.SelectionMode.NoSelection
except AttributeError:  # pragma: no cover
    SINGLE_SELECTION = QAbstractItemView.SingleSelection  # pragma: no cover
    NO_SELECTION = QAbstractItemView.NoSelection  # pragma: no cover

try:
    ITEM_IS_USER_CHECKABLE = Qt.ItemFlag.ItemIsUserCheckable
    CHECKED_STATE = Qt.CheckState.Checked
    UNCHECKED_STATE = Qt.CheckState.Unchecked
    USER_ROLE = Qt.ItemDataRole.UserRole
except AttributeError:  # pragma: no cover
    ITEM_IS_USER_CHECKABLE = Qt.ItemIsUserCheckable  # pragma: no cover
    CHECKED_STATE = Qt.Checked  # pragma: no cover
    UNCHECKED_STATE = Qt.Unchecked  # pragma: no cover
    USER_ROLE = Qt.UserRole  # pragma: no cover

try:
    DIALOG_ACCEPTED = QDialog.DialogCode.Accepted
    DIALOG_REJECTED = QDialog.DialogCode.Rejected
except AttributeError:  # pragma: no cover
    DIALOG_ACCEPTED = QDialog.Accepted  # pragma: no cover
    DIALOG_REJECTED = QDialog.Rejected  # pragma: no cover

try:
    BUTTON_OK = QDialogButtonBox.StandardButton.Ok
    BUTTON_CANCEL = QDialogButtonBox.StandardButton.Cancel
except AttributeError:  # pragma: no cover
    BUTTON_OK = QDialogButtonBox.Ok  # pragma: no cover
    BUTTON_CANCEL = QDialogButtonBox.Cancel  # pragma: no cover

KANJI_PATTERN = re.compile(r"[\u3400-\u9FFF\uF900-\uFAFF]")

SQLITE_MAX_VARIABLES = 900

BUCKET_TAG_KEYS: Tuple[str, str, str] = (
    "reviewed_vocab",
    "unreviewed_vocab",
    "no_vocab",
)


@dataclass
class VocabNoteTypeConfig:
    name: str
    fields: List[str] = field(default_factory=list)


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
    auto_suspend_vocab: bool
    auto_suspend_tag: str


@dataclass
class KanjiUsageInfo:
    reviewed: bool = False
    first_review_order: Optional[int] = None
    first_review_due: Optional[int] = None
    first_new_due: Optional[int] = None
    first_new_order: Optional[int] = None
    vocab_occurrences: int = 0


class KanjiVocabSyncManager:
    """Core coordinator for the KanjiCards add-on."""

    def __init__(self) -> None:  # pragma: no cover
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
        self._pre_answer_card_state: Dict[int, Dict[str, Optional[int]]] = {}
        self._last_question_card_id: Optional[int] = None
        self._debug_path: Optional[str] = None
        self._debug_enabled = False
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
    def _profile_config_path(self) -> Optional[str]:  # pragma: no cover
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

    def _debug(self, message: str, **extra: object) -> None:  # pragma: no cover
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

    def _load_profile_config(self) -> Dict[str, Any]:  # pragma: no cover
        path = self._profile_config_path()
        if not path or not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as err:  # noqa: BLE001
            if not self._profile_config_error_logged:
                print(f"[KanjiCards] Failed to load profile config: {err}")
                self._profile_config_error_logged = True
            return {}
        self._profile_config_error_logged = False
        if isinstance(data, dict):
            return data
        return {}

    def _load_profile_config_or_seed(self, global_cfg: Dict[str, Any]) -> Dict[str, Any]:  # pragma: no cover
        path = self._profile_config_path()
        if not path:
            return {}
        if not os.path.exists(path):
            base_raw = global_cfg if isinstance(global_cfg, dict) else {}
            seed_cfg = self._config_from_raw(base_raw)
            self._write_profile_config(self._serialize_config(seed_cfg))
        return self._load_profile_config()

    def _write_profile_config(self, data: Dict[str, Any]) -> None:  # pragma: no cover
        path = self._profile_config_path()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, ensure_ascii=False)
        except Exception as err:  # noqa: BLE001
            print(f"[KanjiCards] Failed to write profile config: {err}")

    def _merge_config_sources(self, global_cfg: Dict[str, Any], profile_cfg: Dict[str, Any]) -> Dict[str, Any]:  # pragma: no cover
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
        for logical_name in ("kanji", "definition", "stroke_count", "kunyomi", "onyomi", "frequency"):
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
                vocab_cfg.append(
                    VocabNoteTypeConfig(
                        name=item.get("note_type", ""),
                        fields=fields,
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
            auto_suspend_vocab=bool(raw.get("auto_suspend_vocab", False)),
            auto_suspend_tag=raw.get("auto_suspend_tag", "kanjicards_unreviewed"),
        )

    def _serialize_config(self, cfg: AddonConfig) -> Dict[str, Any]:
        return {
            "vocab_note_types": [
                {"note_type": item.name, "fields": list(item.fields)}
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
            "auto_suspend_vocab": bool(cfg.auto_suspend_vocab),
            "auto_suspend_tag": cfg.auto_suspend_tag,
        }

    def load_config(self) -> AddonConfig:  # pragma: no cover
        global_raw_obj = self.mw.addonManager.getConfig(__name__)
        global_raw = global_raw_obj if isinstance(global_raw_obj, dict) else {}
        profile_raw = self._load_profile_config_or_seed(global_raw)
        raw = self._merge_config_sources(global_raw, profile_raw)
        return self._config_from_raw(raw)

    def save_config(self, cfg: AddonConfig) -> None:  # pragma: no cover
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
    def _ensure_menu_actions(self) -> None:  # pragma: no cover
        menu = self.mw.form.menuTools
        sync_action = menu.addAction("Sync Kanji Cards with Vocab")
        sync_action.triggered.connect(self.run_sync)
        self._sync_action = sync_action

        settings_action = menu.addAction("KanjiCards Settings")
        settings_action.triggered.connect(self.show_settings)
        self._settings_action = settings_action

    def _install_hooks(self) -> None:  # pragma: no cover
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
        self._install_sync_hook()

    def _install_sync_hook(self) -> None:  # pragma: no cover
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

    def show_settings(self) -> None:  # pragma: no cover
        dialog = KanjiVocabSyncSettingsDialog(self, self.load_config())
        dialog.exec()

    # ------------------------------------------------------------------
    # Sync routine
    # ------------------------------------------------------------------
    def run_sync(self) -> None:  # pragma: no cover
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
        try:
            stats = self._sync_internal(progress_tracker=progress_tracker)
        except Exception as err:  # noqa: BLE001
            self.mw.progress.finish()
            show_critical(f"KanjiCards sync failed:\n{err}")
            return None
        else:
            self.mw.progress.finish()
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

    def _sync_internal(self, *, progress_tracker: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        cfg = self.load_config()
        collection = self.mw.col
        if collection is None:
            raise RuntimeError("Collection not available")

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

        vocab_field_map = {model["id"]: field_indexes for model, field_indexes in vocab_models}
        self._progress_step(progress_tracker, "Updating vocabulary suspension…")
        suspension_stats = self._update_vocab_suspension(
            collection,
            cfg,
            vocab_field_map,
            existing_notes,
        )
        stats["vocab_suspended"] += suspension_stats.get("vocab_suspended", 0)
        stats["vocab_unsuspended"] += suspension_stats.get("vocab_unsuspended", 0)

        return stats

    def _on_reviewer_did_show_question(self, card: Any, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
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

    def _on_reviewer_did_answer_card(self, card: Any, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
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
                print(f"[KanjiCards] realtime sync error: {err}")
                self._realtime_error_logged = True

    def _process_reviewed_card(self, card: Any) -> None:  # pragma: no cover
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
        was_new = (prev_queue == 0) or (prev_queue is None and prev_type == 0)
        if not was_new:
            self._debug(
                "realtime/skip",
                reason="not_new",
                prev_type=prev_type,
                prev_queue=prev_queue,
            )
            return

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

        vocab_field_map = {model["id"]: indexes for model, indexes in vocab_map.values()}
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
            force_chars_reviewed=kanji_chars,
        )

        self._realtime_error_logged = False

    def _on_sync_event(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        if self._suppress_next_auto_sync:
            self._suppress_next_auto_sync = False
            return
        cfg = self.load_config()
        if not cfg.auto_run_on_sync:
            return
        if not self.mw or not self.mw.col:
            return

        def trigger() -> None:
            if not self.mw or not self.mw.col:
                return
            busy_check = getattr(self.mw.progress, "busy", None)
            if callable(busy_check) and busy_check():
                QTimer.singleShot(200, trigger)
                return
            self._realtime_error_logged = False
            stats = self.run_sync()
            if stats and self._stats_warrant_sync(stats):
                def followup() -> None:
                    if self._trigger_followup_sync():
                        self._suppress_next_auto_sync = True

                QTimer.singleShot(200, followup)

        try:
            self.mw.taskman.run_on_main(trigger)
        except Exception:
            trigger()

    def _stats_warrant_sync(self, stats: Dict[str, object]) -> bool:  # pragma: no cover
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

    def _trigger_followup_sync(self) -> bool:  # pragma: no cover
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
    ) -> List[Tuple[NotetypeDict, List[int]]]:
        vocab_models: List[Tuple[NotetypeDict, List[int]]] = []
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
            vocab_models.append((model, field_indexes))
        return vocab_models

    def _get_vocab_model_map(
        self,
        collection: Collection,
        cfg: AddonConfig,
    ) -> Dict[int, Tuple[NotetypeDict, List[int]]]:
        key = tuple(
            sorted(
                (entry.name, tuple(entry.fields))
                for entry in cfg.vocab_note_types
                if entry.name
            )
        )
        cache = self._vocab_model_cache
        if cache and cache.get("key") == key:
            return cache["mapping"]

        vocab_models = self._resolve_vocab_models(collection, cfg)
        mapping = {model["id"]: (model, field_indexes) for model, field_indexes in vocab_models}
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

    def _collect_vocab_usage(  # pragma: no cover
        self,
        collection: Collection,
        vocab_models: Sequence[Tuple[NotetypeDict, List[int]]],
        cfg: AddonConfig,
    ) -> Dict[str, KanjiUsageInfo]:
        usage: Dict[str, KanjiUsageInfo] = {}
        review_order = 0
        new_order = 0
        for model, field_indexes in vocab_models:
            if not field_indexes:
                continue
            sql = (
                "SELECT notes.id, notes.flds, "
                "MAX(CASE WHEN cards.type != 0 THEN 1 ELSE 0 END) AS has_reviewed, "
                "MIN(CASE WHEN cards.queue = 0 THEN cards.due END) AS min_new_due, "
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
            rows.sort(key=lambda row: (
                0 if row[3] is not None else 1,
                row[3] if row[3] is not None else 0,
                row[0],
            ))

            active_map: Dict[int, bool] = {}
            auto_suspend_tag = cfg.auto_suspend_tag.strip()
            auto_suspend_tag_lower = auto_suspend_tag.lower()
            if cfg.ignore_suspended_vocab and rows:
                note_ids = [row[0] for row in rows]
                active_map = self._load_note_active_status(collection, note_ids)

            for note_id, flds, has_reviewed, min_new_due, min_review_due in rows:
                if cfg.ignore_suspended_vocab:
                    has_active = active_map.get(note_id, False)
                    if not has_active:
                        if not auto_suspend_tag_lower:
                            continue
                        try:
                            note_obj = _get_note(collection, note_id)
                        except Exception:  # noqa: BLE001
                            continue
                        note_tags_lower = {tag.lower() for tag in getattr(note_obj, "tags", [])}
                        if auto_suspend_tag_lower not in note_tags_lower:
                            continue
                reviewed_flag = bool(has_reviewed)
                review_rank = None
                if reviewed_flag:
                    review_rank = review_order
                    review_order += 1

                new_due_value: Optional[int] = None
                if min_new_due is not None:
                    try:
                        new_due_value = int(min_new_due)
                    except Exception:
                        new_due_value = None

                new_rank = None
                if new_due_value is not None:
                    new_rank = new_order
                    new_order += 1

                review_due_value: Optional[int] = None
                if min_review_due is not None:
                    try:
                        review_due_value = int(min_review_due)
                    except Exception:
                        review_due_value = None

                fields = flds.split("\x1f")
                seen_in_note: Set[str] = set()
                for field_index in field_indexes:
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
                                info.first_review_order is None
                                or review_rank < info.first_review_order
                            ):
                                info.first_review_order = review_rank
                            if review_due_value is not None and (
                                info.first_review_due is None
                                or review_due_value < info.first_review_due
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
            if frequency_field_name and dictionary:
                entry = dictionary.get(kanji_char)
                if isinstance(entry, dict):
                    if self._update_frequency_field(note, frequency_field_name, entry.get("frequency")):
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
        review_order = info.first_review_order if info.first_review_order is not None else big
        review_due = info.first_review_due if info.first_review_due is not None else big
        new_order = info.first_new_order if info.first_new_order is not None else big
        new_due = info.first_new_due if info.first_new_due is not None else due_value
        if new_due is None:
            new_due = big
        due_sort = due_value if due_value is not None else big
        has_frequency = frequency is not None
        freq_value = frequency if has_frequency else big

        if has_vocab and info.reviewed:
            vocab_tuple: Tuple = (
                0,
                review_due,
                freq_value,
                review_order,
                new_due,
                new_order,
                due_sort,
                card_id,
            )
        elif has_vocab:
            vocab_tuple = (
                1,
                new_due,
                freq_value,
                new_order,
                review_order,
                card_id,
            )
        else:
            vocab_tuple = (
                2,
                0 if has_frequency else 1,
                freq_value,
                due_sort,
                card_id,
            )

        bucket_id = int(vocab_tuple[0])

        vocab_count = info.vocab_occurrences if has_vocab else 0

        if mode == "vocab":
            return vocab_tuple, bucket_id

        if mode == "vocab_frequency":
            appearance_tuple = (-vocab_count, *vocab_tuple)
            return appearance_tuple, bucket_id

        # mode == "frequency"
        if has_frequency:
            return (
                0,
                freq_value,
                card_id,
            ), bucket_id

        bucket, *rest = vocab_tuple
        return (1 + bucket, *rest), bucket_id

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
        fields_meta = kanji_model.get("flds") if isinstance(kanji_model, dict) else None
        freq_index = kanji_field_indexes.get("frequency")
        if isinstance(freq_index, int) and isinstance(fields_meta, list) and 0 <= freq_index < len(fields_meta):
            field_meta = fields_meta[freq_index]
            if isinstance(field_meta, dict):
                freq_name = field_meta.get("name")
                if isinstance(freq_name, str):
                    frequency_field_name = freq_name

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
                if frequency_field_name and isinstance(dictionary_entry, dict):
                    if self._update_frequency_field(note, frequency_field_name, dictionary_entry.get("frequency")):
                        note.flush()
                if info is not None:
                    self._update_kanji_status_tags(
                        note,
                        cfg,
                        has_vocab=True,
                        has_reviewed_vocab=info.reviewed,
                    )
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
            )
            stats["tag_removed"] = removed
            stats["resuspended"] = resuspended

        return stats

    def _compute_kanji_reviewed_flags(
        self,
        collection: Collection,
        existing_notes: Dict[str, int],
    ) -> Dict[str, bool]:
        if not existing_notes:
            return {}
        note_ids = list(dict.fromkeys(existing_notes.values()))
        if not note_ids:
            return {}
        rows: List[Tuple[int, int]] = []
        for batch_index, batch_ids in enumerate(_chunk_sequence(note_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            rows.extend(
                _db_all(
                    collection,
                    f"SELECT nid, MAX(CASE WHEN type != 0 THEN 1 ELSE 0 END) FROM cards WHERE nid IN ({placeholders}) GROUP BY nid",
                    *batch_ids,
                    context=f"compute_kanji_reviewed_flags/batch{batch_index}",
                )
            )
        status_by_note: Dict[int, bool] = {nid: bool(flag) for nid, flag in rows}
        return {char: status_by_note.get(note_id, False) for char, note_id in existing_notes.items()}

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
    ) -> Dict[int, List[Tuple[int, int]]]:
        unique_ids = list(dict.fromkeys(note_ids))
        if not unique_ids:
            return {}
        rows: List[Tuple[int, int, int]] = []
        for batch_index, batch_ids in enumerate(_chunk_sequence(unique_ids, SQLITE_MAX_VARIABLES)):
            placeholders = ",".join("?" for _ in batch_ids)
            rows.extend(
                _db_all(
                    collection,
                    f"SELECT id, nid, queue FROM cards WHERE nid IN ({placeholders})",
                    *batch_ids,
                    context=f"load_card_status_for_notes/batch{batch_index}",
                )
            )
        card_map: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
        for card_id, nid, queue in rows:
            card_map[nid].append((card_id, queue))
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

    def _update_vocab_suspension(  # pragma: no cover
        self,
        collection: Collection,
        cfg: AddonConfig,
        vocab_field_map: Dict[int, List[int]],
        existing_notes: Dict[str, int],
        target_chars: Optional[Set[str]] = None,
        force_chars_reviewed: Optional[Set[str]] = None,
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
            force="".join(sorted(force_chars_reviewed)) if force_chars_reviewed else "",
        )
        notes_info = self._collect_vocab_note_chars(collection, vocab_field_map, target_chars)
        if not notes_info:
            self._debug("realtime/update_empty", target=target_display)
            return stats
        tag_lower = tag.lower()
        kanji_reviewed = self._compute_kanji_reviewed_flags(collection, existing_notes)
        if force_chars_reviewed:
            for char in force_chars_reviewed:
                if char:
                    kanji_reviewed[char] = True
            self._debug(
                "realtime/status",
                target="".join(sorted(force_chars_reviewed)),
                notes=len(notes_info),
                tag=tag,
            )
        card_map = self._load_card_status_for_notes(collection, notes_info.keys())

        for note_id, (chars, tag_set) in notes_info.items():
            tag_set_lower = {value.lower() for value in tag_set}
            note_has_tag = tag_lower in tag_set_lower
            requires_suspend = any(not kanji_reviewed.get(char, False) for char in chars)
            cards = card_map.get(note_id, [])
            if cfg.auto_suspend_vocab:
                if requires_suspend:
                    self._debug(
                        "realtime/keep_suspended",
                        note_id=note_id,
                        chars="".join(sorted(chars)),
                    )
                    unsuspended_cards = [card_id for card_id, queue in cards if queue != -1]
                    if not unsuspended_cards:
                        continue
                    note = _get_note(collection, note_id)
                    suspended_count = _resuspend_note_cards(collection, note)
                    if suspended_count > 0:
                        stats["vocab_suspended"] += suspended_count
                        existing_lower = {value.lower() for value in note.tags}
                        if tag_lower not in existing_lower:
                            _add_tag(note, tag)
                            note.flush()
                    continue

                if not note_has_tag:
                    continue

                suspended_cards = [card_id for card_id, queue in cards if queue == -1]
                note = _get_note(collection, note_id)
                changed = False
                if suspended_cards:
                    self._debug(
                        "realtime/unsuspend",
                        note_id=note_id,
                        chars="".join(sorted(chars)),
                        count=len(suspended_cards),
                    )
                    _unsuspend_cards(collection, suspended_cards)
                    stats["vocab_unsuspended"] += len(suspended_cards)
                    changed = True
                if _remove_tag_case_insensitive(note, tag):
                    changed = True
                if changed:
                    note.flush()
                continue

            if not note_has_tag:
                continue

            note = _get_note(collection, note_id)
            suspended_cards = [card_id for card_id, queue in cards if queue == -1]
            changed = False
            if suspended_cards:
                _unsuspend_cards(collection, suspended_cards)
                stats["vocab_unsuspended"] += len(suspended_cards)
                changed = True
            if _remove_tag_case_insensitive(note, tag):
                changed = True
            if changed:
                note.flush()

        return stats

    def _create_kanji_note(  # pragma: no cover
        self,
        collection: Collection,
        kanji_model: NotetypeDict,
        field_indexes: Dict[str, int],
        entry: Dict[str, object],
        kanji_char: str,
        existing_tag: str,
        created_tag: str,
        cfg: AddonConfig,
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

    def _assign_field(self, note: Note, field_name: Optional[str], value: str) -> None:  # pragma: no cover
        if not field_name:
            return
        note[field_name] = value or ""

    def _format_frequency_value(self, value: object) -> str:  # pragma: no cover
        if value is None:
            return ""
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return str(int(value))
        if isinstance(value, str):
            return value.strip()
        return str(value)

    def _update_frequency_field(  # pragma: no cover
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

    def _format_readings(self, value: object) -> str:  # pragma: no cover
        if isinstance(value, list):
            return "; ".join(str(item) for item in value if item)
        return str(value or "")

    def _unsuspend_note_cards_if_needed(  # pragma: no cover
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

    def _resolve_deck_id(self, collection: Collection, model: NotetypeDict, cfg: AddonConfig) -> int:  # pragma: no cover
        if cfg.kanji_deck_name:
            deck_id = self._lookup_deck_id(collection, cfg.kanji_deck_name)
            if deck_id:
                self._missing_deck_logged = False
                return deck_id
            if not self._missing_deck_logged:
                print(
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

    def _lookup_deck_id(self, collection: Collection, name: str) -> Optional[int]:  # pragma: no cover
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

    def _deck_entry_name(self, entry: Any) -> Optional[str]:  # pragma: no cover
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


class KanjiVocabSyncSettingsDialog(QDialog):  # pragma: no cover
    """Settings dialog for configuring the add-on."""

    def __init__(self, manager: KanjiVocabSyncManager, config: AddonConfig) -> None:  # pragma: no cover
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
        self.auto_sync_check = QCheckBox("Run automatically after sync")
        self.auto_sync_check.setChecked(self.config.auto_run_on_sync)
        self.ignore_suspended_check = QCheckBox("Ignore suspended vocab cards")
        self.ignore_suspended_check.setChecked(self.config.ignore_suspended_vocab)
        self.auto_suspend_check = QCheckBox("Suspend vocab with unreviewed kanji")
        self.auto_suspend_check.setChecked(self.config.auto_suspend_vocab)
        self.auto_suspend_tag_edit = QLineEdit(self.config.auto_suspend_tag)
        self.auto_suspend_tag_edit.setEnabled(self.config.auto_suspend_vocab)
        self.auto_suspend_check.toggled.connect(self.auto_suspend_tag_edit.setEnabled)
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
        form.addRow("", self.auto_sync_check)
        form.addRow("", self.ignore_suspended_check)
        form.addRow("", self.auto_suspend_check)
        form.addRow("Suspension tag", self.auto_suspend_tag_edit)
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
        ]:
            combo = QComboBox()
            self.kanji_field_combos[logical_field] = combo
            layout.addRow(label, combo)

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
        for logical, combo in self.kanji_field_combos.items():
            combo.clear()
            combo.addItem("<Not set>")
            if not model:
                continue
            field_names = [fld["name"] for fld in model["flds"]]
            combo.addItems(field_names)
            current_name = self.config.kanji_note_type.fields.get(logical, "")
            try:
                combo.setCurrentIndex(field_names.index(current_name) + 1)
            except ValueError:
                combo.setCurrentIndex(0)

    def _current_kanji_model(self) -> Optional[NotetypeDict]:  # pragma: no cover
        index = self.kanji_model_combo.currentIndex()
        if index < 0 or index >= len(self.models_by_index):
            return None
        return self.models_by_index[index]

    def _reload_vocab_entries(self) -> None:  # pragma: no cover
        self.vocab_list.clear()
        for entry in self.config.vocab_note_types:
            fields = ", ".join(entry.fields)
            item = QListWidgetItem(f"{entry.name} — {fields}")
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
        self.config.auto_run_on_sync = self.auto_sync_check.isChecked()
        self.config.ignore_suspended_vocab = self.ignore_suspended_check.isChecked()
        auto_suspend_enabled = self.auto_suspend_check.isChecked()
        auto_suspend_tag = self.auto_suspend_tag_edit.text().strip()
        if auto_suspend_enabled and not auto_suspend_tag:
            show_warning("Provide a suspension tag when auto-suspend is enabled.")
            return False
        self.config.auto_suspend_vocab = auto_suspend_enabled
        self.config.auto_suspend_tag = auto_suspend_tag
        self.config.reorder_mode = self.reorder_combo.currentData() or "vocab"
        self.config.bucket_tags = {
            "reviewed_vocab": self.bucket_reviewed_tag_edit.text().strip(),
            "unreviewed_vocab": self.bucket_unreviewed_tag_edit.text().strip(),
            "no_vocab": self.bucket_no_vocab_tag_edit.text().strip(),
        }
        self.config.only_new_vocab_tag = self.only_new_vocab_tag_edit.text().strip()
        self.config.no_vocab_tag = self.no_vocab_tag_edit.text().strip()
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
        manager: KanjiVocabSyncManager,
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
        self.existing = VocabNoteTypeConfig(name=model["name"], fields=selected_fields)
        self.accept()

    def get_result(self) -> VocabNoteTypeConfig:  # pragma: no cover
        if not self.existing:
            raise RuntimeError("Dialog accepted without configuration")
        return self.existing


_manager: Optional[KanjiVocabSyncManager] = None


def _initialize_manager() -> None:
    global _manager
    if _manager is None and mw is not None:
        _manager = KanjiVocabSyncManager()


def on_profile_loaded() -> None:
    _initialize_manager()


def on_main_window_did_init() -> None:
    _initialize_manager()


gui_hooks.profile_did_open.append(on_profile_loaded)
gui_hooks.main_window_did_init.append(on_main_window_did_init)


# ----------------------------------------------------------------------
# Backwards compatibility helpers
# ----------------------------------------------------------------------
def _add_note(collection: Collection, note: Note, deck_id: Optional[int] = None) -> bool:  # pragma: no cover
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


def _unsuspend_cards(collection: Collection, card_ids: Sequence[int]) -> None:  # pragma: no cover
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


def _resuspend_note_cards(collection: Collection, note: Note) -> int:  # pragma: no cover
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


def _db_all(  # pragma: no cover
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


def _db_execute(  # pragma: no cover
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


def _log_db_error(  # pragma: no cover
    operation: str,
    sql: str,
    params: Sequence[object],
    context: str,
    err: Exception,
) -> None:
    prefix = "[KanjiCards] db.%s failed" % operation
    if context:
        prefix += f" ({context})"
    print(prefix + f": {err}")
    print(f"  SQL: {sql}")
    if params:
        print(f"  Params: {params}")


def _chunk_sequence(values: Sequence[int], chunk_size: int) -> Iterator[List[int]]:
    """Yield slices limited by SQLite parameter cap."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for start in range(0, len(values), chunk_size):
        yield list(values[start : start + chunk_size])


def _new_note(collection: Collection, model: NotetypeDict) -> Note:  # pragma: no cover
    handler = getattr(collection, "new_note", None)
    if callable(handler):
        return handler(model)
    return collection.newNote(model)


def _get_note(collection: Collection, note_id: int) -> Note:  # pragma: no cover
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
