# KanjiCards

KanjiCards is an Anki add-on that keeps a configured kanji note type in sync with the kanji that appear in your reviewed vocabulary notes. It helps surface which characters already have dedicated study cards, automatically fills in missing kanji notes from a dictionary, and unsuspends kanji cards that should be active for newly encountered vocabulary.

## Key Features
- Scans only **reviewed** vocabulary cards (cards with revlog entries) from the note types you configure.
- Finds all kanji characters in the selected vocab fields and ensures matching kanji notes exist.
- Tags existing kanji notes with a configurable tag; newly created kanji notes receive both the “existing” tag and an additional “auto-created” tag.
- Automatically populates kanji notes with character, meaning, stroke count, kunyomi, and onyomi data taken from a configured dictionary (KANJIDIC2 XML or a JSON mapping).
- Unsuspends kanji cards (unless they carry the leech tag) so they return to study if you already have a note for that character.
- While you review configured vocab cards, the add-on performs the same tagging/creation/unsuspension automatically in real time.
- Offers a GUI settings dialog from Anki’s **Tools** menu for configuring vocab note types, the kanji note type/field mapping, tags, and dictionary file.

## Installation
1. Copy the contents of this repository into Anki’s `addons21` directory under a folder named `kanjicards` (or install via AnkiWeb once published).
2. Restart Anki so it loads the add-on.

## Configuration
1. In Anki, open **Tools → Kanji Vocab Sync Settings**.
2. **General tab**
   - Set the tag to apply to existing kanji notes (default: `has_vocab_kanji`).
   - Set the tag added to auto-created kanji notes (default: `auto_kanji_card`).
   - Choose the dictionary file. You can supply either:
     - A JSON file mapping kanji to the fields `definition`, `stroke_count`, `kunyomi`, and `onyomi`, or
     - A full KANJIDIC2 XML file (recommended). The add-on parses the XML and extracts the first stroke count, all Japanese on/kun readings, and English meanings.
   - Optionally pick a specific deck for newly created kanji notes. Leave blank to fall back to the kanji note type’s default deck or your current deck.
3. **Kanji note tab**
   - Select the kanji note type and assign which fields store each piece of data (kanji, definition, stroke count, kunyomi, onyomi).
4. **Vocab notes tab**
   - Add one or more vocabulary note types and select the fields that should be scanned for kanji characters.
5. Click **OK** to save your settings.

## Usage
- Choose **Tools → Sync Kanji Cards with Vocab** whenever you want to sync.
- The add-on reports how many kanji it scanned, how many existing notes were tagged, how many cards were unsuspended, how many new kanji notes were created, and whether any kanji were missing in the dictionary source.
- Newly created kanji notes are added to the deck associated with the kanji note type (or Anki’s current/default deck if none is set).

## Dictionary Data and Licensing
If you bundle the official KANJIDIC2 file with this add-on or your deck, you must comply with the Electronic Dictionary Research & Development Group’s licensing terms:

- **KANJIDIC2 data** (including SKIP codes) is released under the Creative Commons Attribution–ShareAlike 4.0 International License (CC BY-SA 4.0).
- You must give appropriate credit, provide a link to the license, and indicate if changes were made.
- If you redistribute a modified version of the dictionary data, it must remain under CC BY-SA 4.0.

Recommended attribution text:
>
> This add-on bundles data from the KANJIDIC2 project. © Electronic Dictionary Research & Development Group, used under CC BY-SA 4.0. No changes were made (unless you modify it—then describe the modifications).

See the [EDRDG license page](https://www.edrdg.org/edrdg/licence.html) and the [Creative Commons CC BY-SA 4.0 summary](https://creativecommons.org/licenses/by-sa/4.0/) for full details.

If you choose to ship an alternative JSON dictionary, document its source and licensing accordingly.

## Code License
Unless noted otherwise, the source code for KanjiCards is released under the MIT License. See `LICENSE` for details.

## Contributing
Bug reports and pull requests are welcome. Please mention your Anki version, operating system, and any relevant console output when filing an issue.

## Acknowledgements
- The Anki developer community for the add-on API and documentation.
- Electronic Dictionary Research & Development Group for maintaining the KANJIDIC2 dataset.
