# KanjiCards

KanjiCards keeps a kanji note type aligned with the characters that appear in the vocabulary notes you actually study. It makes sure every kanji has a card, fills those cards with dictionary data, and keeps their suspension/tag status in step with your reviews.

## What It Does
- Scans the fields you mark on each vocab note type, collects every kanji it finds, and tracks whether those cards have review history (optionally skipping suspended vocab cards).
- Creates missing kanji notes and populates kanji, meanings, stroke counts, readings, and optional frequency data from a KANJIDIC2 XML file or a JSON mapping.
- Tags matching kanji notes so you can see which ones already connect to vocab (`existing_tag`), which ones the add-on created (`created_tag`), and which ones it unsuspended (`unsuspended_tag`).
- Keeps kanji status tags up to date: optional markers for kanji that only appear in new vocab (`only_new_vocab_tag`) or no longer appear anywhere (`no_vocab_tag`), plus bucket tags for reviewed / unreviewed / no-vocab kanji if you configure them.
- Unsuspends kanji cards that should come back for study and resuspends them (removing tags) if no configured vocab uses the character anymore.
- Optionally auto-suspends vocab cards whose kanji are still unseen and unsuspends them once the matching kanji have been reviewed.
- Reorders new kanji cards so they appear either by KANJIDIC frequency, by how often they show up in your vocab, or by the order you will encounter them in vocabulary.

## Getting Started
1. Copy this folder into Anki’s `addons21` directory under `kanjicards` (or install it from AnkiWeb when available).
2. Place your dictionary file (default: `kanjidic2.xml`) inside the add-on folder or provide an absolute path.
3. Restart Anki so the add-on loads.

## Configure It
Open **Tools → KanjiCards Settings**.
- **General tab**: pick the tags you want to use, choose the dictionary file, select the destination deck for new kanji notes (or keep the default), decide whether to update during reviews, run automatically after sync, ignore suspended vocab, auto-suspend vocab that still contains unreviewed kanji, and pick a reorder mode or bucket tags if you use them.
- **Kanji note tab**: choose the kanji note type and map which fields hold the literal, meaning, stroke count, readings, and optional frequency.
- **Vocab notes tab**: add each vocabulary note type you care about and choose the text fields that should be scanned for kanji.
Save to persist both the global config and your per-profile overrides.

## Running a Recalc
- Use **Tools → Recalc Kanji Cards with Vocab** to run the process on demand.
- The add-on reports how many kanji it scanned, how many notes were created or tagged, how many cards were suspended/unsuspended, and any characters missing from the dictionary.
- Newly created kanji notes go to the deck you selected (or the note type’s default deck if left blank).
- With **Update during reviews** enabled, KanjiCards updates kanji tags and vocab suspension right after you study a new kanji card, so freshly reviewed characters take effect without a manual recalc.
- With **Run automatically after sync** enabled, KanjiCards runs a recalc as soon as the collection finishes syncing; if changes were made, it asks Anki to sync again so the updates propagate.

## Dictionary Data and Licensing
KANJIDIC2 data (including SKIP codes) is released under the Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0). If you redistribute the file with this add-on or a deck, you must provide attribution, link to the license, and share any modifications under the same license.

Suggested attribution:
> This add-on bundles data from the KANJIDIC2 project. © Electronic Dictionary Research & Development Group, used under CC BY-SA 4.0.

See the [EDRDG license page](https://www.edrdg.org/edrdg/licence.html) and the [Creative Commons CC BY-SA 4.0 summary](https://creativecommons.org/licenses/by-sa/4.0/) for details. If you ship an alternative JSON dictionary, document its source and licensing as needed.

## License
KanjiCards is released under the MIT License. See `LICENSE` for the full text.
