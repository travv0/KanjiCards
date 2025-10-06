"""Kanji Vocab Sync Add-on.

This add-on inspects configured vocabulary notes that the user has reviewed
and ensures each kanji found in those notes has a corresponding kanji card.
Existing kanji cards receive a configurable tag, and missing ones are created
automatically using dictionary data and tagged accordingly.
"""
from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

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

# Messaging helpers differ between Qt versions, so prefer new-style names.
try:
    from aqt.utils import show_critical, show_info, show_warning, tooltip
except ImportError:  # Legacy Anki versions
    from aqt.utils import showCritical as show_critical
    from aqt.utils import showInfo as show_info
    from aqt.utils import showWarning as show_warning
    from aqt.utils import tooltip

try:  # PyQt6-style enums
    SINGLE_SELECTION = QAbstractItemView.SelectionMode.SingleSelection
    NO_SELECTION = QAbstractItemView.SelectionMode.NoSelection
except AttributeError:  # PyQt5 fallback
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
    dictionary_file: str
    kanji_deck_name: str
    auto_run_on_sync: bool
    realtime_review: bool
    unsuspended_tag: str
    reorder_mode: str


@dataclass
class KanjiUsageInfo:
    reviewed: bool = False
    first_review_order: Optional[int] = None
    first_new_due: Optional[int] = None
    first_new_order: Optional[int] = None


class KanjiVocabSyncManager:
    """Core coordinator for the Kanji Vocab Sync add-on."""

    def __init__(self) -> None:
        if not mw:
            raise RuntimeError("Kanji Vocab Sync requires Anki main window")
        self.mw = mw
        self._dictionary_cache: Optional[Dict[str, Any]] = None
        self._existing_notes_cache: Optional[Dict[str, Any]] = None
        self._kanji_model_cache: Optional[Dict[str, Any]] = None
        self._vocab_model_cache: Optional[Dict[str, Any]] = None
        self._realtime_error_logged = False
        self._missing_deck_logged = False
        self._sync_hook_installed = False
        self._sync_hook_target: Optional[str] = None
        self.addon_name = self.mw.addonManager.addonFromModule(__name__)
        if self.addon_name:
            self.addon_dir = os.path.join(self.mw.addonManager.addonsFolder(), self.addon_name)
        else:
            # Fallback for development environments where the addon manager does not know the module.
            self.addon_dir = os.path.dirname(__file__)
        self._ensure_menu_actions()
        self._install_hooks()
        self._install_sync_hook()
        self.mw.addonManager.setConfigAction(__name__, self.show_settings)

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def load_config(self) -> AddonConfig:
        raw = self.mw.addonManager.getConfig(__name__) or {}
        vocab_cfg = [
            VocabNoteTypeConfig(name=item.get("note_type", ""), fields=item.get("fields", []) or [])
            for item in raw.get("vocab_note_types", [])
        ]
        kanji_cfg_raw = raw.get("kanji_note_type", {})
        kanji_cfg = KanjiNoteTypeConfig(
            name=kanji_cfg_raw.get("name", ""),
            fields=kanji_cfg_raw.get("fields", {}),
        )
        return AddonConfig(
            vocab_note_types=vocab_cfg,
            kanji_note_type=kanji_cfg,
            existing_tag=raw.get("existing_tag", "has_vocab_kanji"),
            created_tag=raw.get("created_tag", "auto_kanji_card"),
            dictionary_file=raw.get("dictionary_file", "kanjidic2.xml"),
            kanji_deck_name=raw.get("kanji_deck_name", ""),
            auto_run_on_sync=raw.get("auto_run_on_sync", False),
            realtime_review=raw.get("realtime_review", True),
            unsuspended_tag=raw.get("unsuspended_tag", "kanjicards_unsuspended"),
            reorder_mode=raw.get("reorder_mode", "frequency"),
        )

    def save_config(self, cfg: AddonConfig) -> None:
        raw = {
            "vocab_note_types": [
                {"note_type": item.name, "fields": item.fields}
                for item in cfg.vocab_note_types
            ],
            "kanji_note_type": {
                "name": cfg.kanji_note_type.name,
                "fields": cfg.kanji_note_type.fields,
            },
            "existing_tag": cfg.existing_tag,
            "created_tag": cfg.created_tag,
            "dictionary_file": cfg.dictionary_file,
            "kanji_deck_name": cfg.kanji_deck_name,
            "auto_run_on_sync": bool(cfg.auto_run_on_sync),
            "realtime_review": bool(cfg.realtime_review),
            "unsuspended_tag": cfg.unsuspended_tag,
            "reorder_mode": cfg.reorder_mode,
        }
        self.mw.addonManager.writeConfig(__name__, raw)
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
        sync_action = menu.addAction("Sync Kanji Cards with Vocab")
        sync_action.triggered.connect(self.run_sync)
        self._sync_action = sync_action

        settings_action = menu.addAction("Kanji Vocab Sync Settings")
        settings_action.triggered.connect(self.show_settings)
        self._settings_action = settings_action

    def _install_hooks(self) -> None:
        try:
            gui_hooks.reviewer_did_answer_card.remove(self._on_reviewer_did_answer_card)
        except (ValueError, AttributeError):
            pass
        gui_hooks.reviewer_did_answer_card.append(self._on_reviewer_did_answer_card)
        self._install_sync_hook()

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
        dialog = KanjiVocabSyncSettingsDialog(self, self.load_config())
        dialog.exec()

    # ------------------------------------------------------------------
    # Sync routine
    # ------------------------------------------------------------------
    def run_sync(self) -> None:
        self.mw.checkpoint("Kanji Vocab Sync")
        self.mw.progress.start(label="Scanning reviewed vocabulary notes...", immediate=True)
        try:
            stats = self._sync_internal()
        except Exception as err:  # noqa: BLE001
            self.mw.progress.finish()
            show_critical(f"Kanji Vocab Sync failed:\n{err}")
            return None
        else:
            self.mw.progress.finish()
            self._notify_summary(stats)
            self.mw.reset()
            return stats

    def _sync_internal(self) -> Dict[str, object]:
        cfg = self.load_config()
        collection = self.mw.col
        if collection is None:
            raise RuntimeError("Collection not available")

        if not cfg.kanji_note_type.name:
            raise RuntimeError("Kanji note type is not configured yet")

        kanji_model, kanji_field_indexes, kanji_field_index = self._get_kanji_model_context(collection, cfg)

        vocab_models = self._resolve_vocab_models(collection, cfg)
        if not vocab_models:
            raise RuntimeError("No valid vocabulary note types configured")

        dictionary = self._load_dictionary(cfg.dictionary_file)

        usage_info = self._collect_vocab_usage(collection, vocab_models)
        active_chars = set(usage_info.keys())

        existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)

        stats = self._apply_kanji_updates(
            collection,
            active_chars,
            dictionary,
            kanji_model,
            kanji_field_indexes,
            kanji_field_index,
            cfg,
            existing_notes,
            prune_existing=True,
        )

        if cfg.reorder_mode in {"frequency", "vocab"}:
            self._reorder_new_kanji_cards(
                collection,
                kanji_model,
                kanji_field_index,
                cfg,
                usage_info,
                dictionary,
            )

        return stats

    def _on_reviewer_did_answer_card(self, card: Any, *args: Any, **kwargs: Any) -> None:
        if not card:
            return
        if self.mw.col is None:
            return
        try:
            self._process_reviewed_card(card)
        except Exception as err:  # noqa: BLE001
            if not self._realtime_error_logged:
                print(f"[KanjiCards] realtime sync error: {err}")
                self._realtime_error_logged = True

    def _process_reviewed_card(self, card: Any) -> None:
        collection = self.mw.col
        if collection is None:
            return

        cfg = self.load_config()
        if not cfg.vocab_note_types:
            return
        if not cfg.realtime_review:
            return

        try:
            kanji_model, kanji_field_indexes, kanji_field_index = self._get_kanji_model_context(collection, cfg)
        except RuntimeError:
            # Configuration incomplete; wait until user configures properly.
            return

        vocab_map = self._get_vocab_model_map(collection, cfg)
        if not vocab_map:
            return

        try:
            note = card.note()
        except Exception:  # noqa: BLE001
            return

        model_info = vocab_map.get(note.mid)
        if not model_info:
            return
        _, field_indexes = model_info
        if not field_indexes:
            return

        try:
            fields = list(note.fields)
        except Exception:  # noqa: BLE001
            fields = note.split_fields() if hasattr(note, "split_fields") else []

        kanji_chars: Set[str] = set()
        for field_index in field_indexes:
            if field_index < len(fields):
                kanji_chars.update(KANJI_PATTERN.findall(fields[field_index]))

        if not kanji_chars:
            return

        try:
            dictionary = self._load_dictionary(cfg.dictionary_file)
        except Exception as err:  # noqa: BLE001
            if not self._realtime_error_logged:
                print(f"[KanjiCards] dictionary load failed during review: {err}")
                self._realtime_error_logged = True
            return

        existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)

        self._apply_kanji_updates(
            collection,
            kanji_chars,
            dictionary,
            kanji_model,
            kanji_field_indexes,
            kanji_field_index,
            cfg,
            existing_notes,
        )

        if cfg.reorder_mode in {"frequency", "vocab"}:
            usage_all = self._collect_vocab_usage(collection, list(vocab_map.values()))
            self._reorder_new_kanji_cards(
                collection,
                kanji_model,
                kanji_field_index,
                cfg,
                usage_all,
                dictionary,
            )

        self._realtime_error_logged = False

    def _on_sync_event(self, *args: Any, **kwargs: Any) -> None:
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
                QTimer.singleShot(200, self._trigger_followup_sync)

        try:
            self.mw.taskman.run_on_main(trigger)
        except Exception:
            trigger()

    def _stats_warrant_sync(self, stats: Dict[str, object]) -> bool:
        for key in ("created", "existing_tagged", "unsuspended", "tag_removed", "resuspended"):
            try:
                if int(stats.get(key, 0)) > 0:
                    return True
            except Exception:
                continue
        return False

    def _trigger_followup_sync(self) -> None:
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
                return
        toolbar = getattr(self.mw, "form", None)
        sync_button = getattr(toolbar, "syncButton", None)
        if sync_button is not None:
            try:
                sync_button.animateClick()
            except Exception:
                pass

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

    def _collect_vocab_usage(
        self,
        collection: Collection,
        vocab_models: Sequence[Tuple[NotetypeDict, List[int]]],
    ) -> Dict[str, KanjiUsageInfo]:
        usage: Dict[str, KanjiUsageInfo] = {}
        review_order = 0
        new_order = 0
        for model, field_indexes in vocab_models:
            if not field_indexes:
                continue
            rows = collection.db.all(
                """
                SELECT notes.id,
                       notes.flds,
                       MAX(CASE WHEN cards.reps > 0 THEN 1 ELSE 0 END) AS has_reviewed,
                       MIN(CASE WHEN cards.queue = 0 THEN cards.due END) AS min_new_due
                FROM notes
                JOIN cards ON cards.nid = notes.id
                WHERE notes.mid = ?
                GROUP BY notes.id
                """,
                model["id"],
            )
            rows.sort(key=lambda row: (
                0 if row[3] is not None else 1,
                row[3] if row[3] is not None else 0,
                row[0],
            ))

            for note_id, flds, has_reviewed, min_new_due in rows:
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

                fields = flds.split("\x1f")
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
                        if reviewed_flag:
                            info.reviewed = True
                            if review_rank is not None and (
                                info.first_review_order is None
                                or review_rank < info.first_review_order
                            ):
                                info.first_review_order = review_rank
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
        rows = collection.db.all(
            "SELECT id, flds FROM notes WHERE mid = ?",
            kanji_model["id"],
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

    def _remove_unused_tags(
        self,
        collection: Collection,
        existing_notes: Dict[str, int],
        tag: str,
        unsuspend_tag: str,
        active_chars: Set[str],
    ) -> Tuple[int, int]:
        removed = 0
        resuspended_total = 0
        for kanji_char, note_id in existing_notes.items():
            if kanji_char in active_chars:
                continue
            note = _get_note(collection, note_id)
            changed = False
            if tag in note.tags:
                _remove_tag(note, tag)
                changed = True
                removed += 1
            resuspended = 0
            if unsuspend_tag and unsuspend_tag in note.tags:
                _remove_tag(note, unsuspend_tag)
                resuspended = _resuspend_note_cards(collection, note)
                if resuspended:
                    changed = True
                    resuspended_total += resuspended
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
    ) -> None:
        mode = cfg.reorder_mode
        if mode not in {"frequency", "vocab"}:
            return

        rows = collection.db.all(
            """
            SELECT cards.id, cards.nid, cards.due, cards.did, cards.mod, cards.usn, notes.flds
            FROM cards
            JOIN notes ON notes.id = cards.nid
            WHERE notes.mid = ? AND cards.queue = 0
            """,
            kanji_model["id"],
        )
        if not rows:
            return

        entries: List[Tuple[Tuple, int, int, int, int]] = []
        for card_id, note_id, due_value, deck_id, original_mod, original_usn, flds in rows:
            fields = flds.split("\x1f")
            if kanji_field_index >= len(fields):
                continue
            kanji_char = fields[kanji_field_index].strip()
            if not kanji_char:
                continue
            info = usage_info.get(kanji_char, KanjiUsageInfo())
            entry = dictionary.get(kanji_char) or {}
            freq_val = entry.get("frequency")
            freq = None
            if isinstance(freq_val, int):
                freq = freq_val
            elif isinstance(freq_val, str) and freq_val.isdigit():
                freq = int(freq_val)

            key = self._build_reorder_key(mode, info, freq, due_value, card_id)
            entries.append((key, card_id, due_value, original_mod, original_usn))

        if not entries:
            return

        now = intTime()
        usn = collection.usn()
        entries.sort(key=lambda item: item[0])
        for new_due, (key, card_id, original_due, original_mod, original_usn) in enumerate(entries):
            new_mod = now if new_due != original_due else original_mod
            new_usn = usn if new_due != original_due else original_usn
            collection.db.execute(
                "UPDATE cards SET due = ?, mod = ?, usn = ? WHERE id = ?",
                new_due,
                new_mod,
                new_usn,
                card_id,
            )

    def _build_reorder_key(
        self,
        mode: str,
        info: KanjiUsageInfo,
        frequency: Optional[int],
        due_value: Optional[int],
        card_id: int,
    ) -> Tuple:
        big = 10**9
        review_order = info.first_review_order if info.first_review_order is not None else big
        new_order = info.first_new_order if info.first_new_order is not None else big
        new_due = info.first_new_due if info.first_new_due is not None else due_value
        if new_due is None:
            new_due = big

        if mode == "frequency":
            if frequency is not None:
                return (0, frequency, new_due, review_order, new_order, card_id)
            return (1, new_due, new_order, review_order, card_id)

        # mode == "vocab"
        if info.reviewed:
            freq_sort = frequency if frequency is not None else big
            return (0, freq_sort, review_order, new_due, new_order, card_id)
        freq_sort = frequency if frequency is not None else big
        return (1, new_due, freq_sort, new_order, card_id)

    def _apply_kanji_updates(
        self,
        collection: Collection,
        kanji_chars: Union[Sequence[str], Set[str]],
        dictionary: Dict[str, Dict[str, object]],
        kanji_model: NotetypeDict,
        kanji_field_indexes: Dict[str, int],
        kanji_field_index: int,
        cfg: AddonConfig,
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
        }

        if not unique_chars:
            return stats

        if existing_notes is None:
            existing_notes = self._get_existing_kanji_notes(collection, kanji_model, kanji_field_index)

        unsuspend_tag = cfg.unsuspended_tag

        for kanji_char in unique_chars:
            if kanji_char in existing_notes:
                note_id = existing_notes[kanji_char]
                tagged, note = self._ensure_note_tagged(collection, note_id, cfg.existing_tag)
                if tagged:
                    stats["existing_tagged"] += 1
                unsuspended = self._unsuspend_note_cards_if_needed(collection, note, unsuspend_tag)
                if unsuspended:
                    stats["unsuspended"] += unsuspended
                continue

            dictionary_entry = dictionary.get(kanji_char)
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

        if prune_existing and cfg.existing_tag:
            removed, resuspended = self._remove_unused_tags(
                collection,
                existing_notes,
                cfg.existing_tag,
                unsuspend_tag,
                unique_chars,
            )
            stats["tag_removed"] = removed
            stats["resuspended"] = resuspended

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

        card_rows = collection.db.all(
            "SELECT id, queue FROM cards WHERE nid = ?",
            note.id,
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


class KanjiVocabSyncSettingsDialog(QDialog):
    """Settings dialog for configuring the add-on."""

    def __init__(self, manager: KanjiVocabSyncManager, config: AddonConfig) -> None:
        super().__init__(manager.mw)
        self.setWindowTitle("Kanji Vocab Sync Settings")
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
    def _build_general_tab(self) -> None:
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
        self.reorder_combo = QComboBox()
        self.reorder_combo.addItem("Frequency (KANJIDIC)", "frequency")
        self.reorder_combo.addItem("Vocabulary order", "vocab")
        current_mode = self.config.reorder_mode if self.config.reorder_mode in {"frequency", "vocab"} else "frequency"
        index = self.reorder_combo.findData(current_mode)
        if index >= 0:
            self.reorder_combo.setCurrentIndex(index)

        form.addRow("Existing kanji tag", self.existing_tag_edit)
        form.addRow("Auto-created kanji tag", self.created_tag_edit)
        form.addRow("Dictionary file", self.dictionary_edit)
        form.addRow("Unsuspended tag", self.unsuspend_tag_edit)
        form.addRow("Kanji deck", self.deck_combo)
        form.addRow("", self.realtime_check)
        form.addRow("", self.auto_sync_check)
        form.addRow("Order new kanji cards", self.reorder_combo)

        self.tabs.addTab(widget, "General")

    def _build_kanji_tab(self) -> None:
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
        ]:
            combo = QComboBox()
            self.kanji_field_combos[logical_field] = combo
            layout.addRow(label, combo)

        self.kanji_model_combo.currentIndexChanged.connect(self._refresh_kanji_field_combos)
        self._refresh_kanji_field_combos()

        self.tabs.addTab(widget, "Kanji note")

    def _build_vocab_tab(self) -> None:
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
    ) -> None:
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

    def _refresh_kanji_field_combos(self) -> None:
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

    def _current_kanji_model(self) -> Optional[NotetypeDict]:
        index = self.kanji_model_combo.currentIndex()
        if index < 0 or index >= len(self.models_by_index):
            return None
        return self.models_by_index[index]

    def _reload_vocab_entries(self) -> None:
        self.vocab_list.clear()
        for entry in self.config.vocab_note_types:
            fields = ", ".join(entry.fields)
            item = QListWidgetItem(f"{entry.name} — {fields}")
            item.setData(USER_ROLE, entry)
            self.vocab_list.addItem(item)

    def _add_vocab_entry(self) -> None:
        dialog = VocabNoteConfigDialog(self.manager)
        if dialog.exec() == DIALOG_ACCEPTED:
            cfg = dialog.get_result()
            self.config.vocab_note_types.append(cfg)
            self._reload_vocab_entries()

    def _edit_vocab_entry(self) -> None:
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

    def _remove_vocab_entry(self) -> None:
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
        self.config.reorder_mode = self.reorder_combo.currentData() or "frequency"
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


class VocabNoteConfigDialog(QDialog):
    """Dialog to configure a single vocabulary note type entry."""

    def __init__(
        self,
        manager: KanjiVocabSyncManager,
        existing: Optional[VocabNoteTypeConfig] = None,
    ) -> None:
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

    def _populate_models(self, selected_name: str) -> None:
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

    def _populate_fields(self) -> None:
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

    def _current_model(self) -> Optional[NotetypeDict]:
        index = self.model_combo.currentIndex()
        if index < 0 or index >= len(self._models):
            return None
        return self._models[index]

    def _on_accept(self) -> None:
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

    def get_result(self) -> VocabNoteTypeConfig:
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
    params: List[object] = [intTime(), collection.usn()] + list(card_ids)
    collection.db.execute(
        f"UPDATE cards SET mod = ?, usn = ?, queue = type WHERE id IN ({placeholders})",
        params,
    )


def _resuspend_note_cards(collection: Collection, note: Note) -> int:
    card_rows = collection.db.all(
        "SELECT id, queue FROM cards WHERE nid = ?",
        note.id,
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
    params: List[object] = [intTime(), collection.usn()] + list(to_suspend)
    collection.db.execute(
        f"UPDATE cards SET mod = ?, usn = ?, queue = -1 WHERE id IN ({placeholders})",
        params,
    )
    return len(to_suspend)


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
