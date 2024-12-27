"""Drive two browser windows on two realtime nodes and capture the README screenshot.

    npm run dev                                  # in another terminal
    python scripts/make_screenshot.py [--url http://localhost:3000]

Ada's window connects to node 0 and Bob's to node 1, so every change shown
crossed between nodes through Redis pub/sub. Everything happens through the
real UI (double-click to add, Enter to edit, drag to move). The script checks
that both windows end with the same notes before saving docs/screenshot.png
(Ada's window) and docs/screenshot-bob.png.

Needs Playwright for Python with Chromium (`pip install playwright` and
`playwright install chromium`).
"""

from __future__ import annotations

import argparse
import json
import random
import string
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

VIEWPORT = {"width": 1440, "height": 860}


def open_window(browser, url: str, user: dict) -> Page:
    context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1)
    context.add_init_script(f"localStorage.setItem('rcb:user', {json.dumps(json.dumps(user))})")
    page = context.new_page()
    page.goto(url)
    page.wait_for_selector(".status.online")
    page.wait_for_selector(".empty, .note")
    return page


def canvas_point(page: Page, x: int, y: int) -> tuple[float, float]:
    box = page.locator(".canvas").bounding_box()
    assert box
    return box["x"] + x, box["y"] + y


def add_note(page: Page, x: int, y: int, text: str, color: str | None = None) -> str:
    before = page.locator(".note").count()
    page.mouse.dblclick(*canvas_point(page, x, y))
    page.wait_for_function(f"document.querySelectorAll('.note').length > {before}")
    note = page.locator(".note.selected")
    note_id = note.get_attribute("data-note-id")
    assert note_id
    if color:
        note.locator(f".swatch.{color}").click()
    page.keyboard.press("Enter")
    page.wait_for_selector(f'.note[data-note-id="{note_id}"] textarea')
    page.keyboard.type(text, delay=8)
    page.keyboard.press("Escape")
    return note_id


def drag_note(page: Page, note_id: str, dx: int, dy: int) -> None:
    box = page.locator(f'.note[data-note-id="{note_id}"]').bounding_box()
    assert box
    x, y = box["x"] + 40, box["y"] + box["height"] - 14
    page.mouse.move(x, y)
    page.mouse.down()
    for i in range(1, 11):
        page.mouse.move(x + dx * i / 10, y + dy * i / 10)
        page.wait_for_timeout(16)
    page.mouse.up()


def note_texts(page: Page) -> dict[str, str]:
    return page.evaluate(
        "Object.fromEntries([...document.querySelectorAll('.note')].map(n => [n.dataset.noteId, n.querySelector('.note-text').innerText]))"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:3000")
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "docs"))
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    board = "sprint-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ada = open_window(browser, f"{args.url}/b/{board}?node=0", {"id": "ada-0001", "name": "Ada Lovelace", "color": "#1971c2"})
        bob = open_window(browser, f"{args.url}/b/{board}?node=1", {"id": "bob-0002", "name": "Bob Smith", "color": "#e8590c"})
        assert ada.locator(".node-select select").input_value() == "0"
        assert bob.locator(".node-select select").input_value() == "1"

        # A small retro board, built by both people at once.
        ada_notes = [
            add_note(ada, 120, 90, "Went well:\nReleased the reconnect catch-up on time"),
            add_note(ada, 380, 90, "Went well:\nZero lost edits in the failover drill", "green"),
            add_note(ada, 120, 330, "Improve:\nFlaky e2e test on CI", "pink"),
        ]
        bob_notes = [
            add_note(bob, 640, 90, "Idea:\nShow who is editing each note", "blue"),
            add_note(bob, 640, 330, "Action:\nAdd Redis Cluster to staging", "purple"),
        ]
        add_note(ada, 900, 90, "Question:\nKeep the op log for 1000 or 5000 ops?", "orange")

        # Bob tidies up: moves one of Ada's notes.
        bob.locator(f'.note[data-note-id="{ada_notes[2]}"]').wait_for()
        drag_note(bob, ada_notes[2], 260, 20)

        # Wait until both windows show the same notes, and nothing is unsaved.
        for page in (ada, bob):
            page.wait_for_function("!document.querySelector('.note.pending') && document.querySelectorAll('.note').length === 6")
        ada.wait_for_function(
            "([id, x]) => Math.abs(parseFloat(document.querySelector(`.note[data-note-id=\"${id}\"]`).style.left) - x) < 2",
            arg=[ada_notes[2], float(bob.locator(f'.note[data-note-id="{ada_notes[2]}"]').evaluate("n => parseFloat(n.style.left)"))],
        )
        assert note_texts(ada) == note_texts(bob), "the two windows disagree"

        # Live state for the screenshot: Bob is typing in a note (Ada sees his
        # lease), and his cursor hovers over the board.
        bob.locator(f'.note[data-note-id="{bob_notes[0]}"]').dblclick()
        bob.wait_for_selector(f'.note[data-note-id="{bob_notes[0]}"] textarea')
        bob.keyboard.press("Control+End")
        bob.keyboard.type(" and where their cursor is", delay=10)
        ada.wait_for_selector(".note-badge")
        bob.mouse.move(*canvas_point(bob, 760, 470))
        bob.mouse.move(*canvas_point(bob, 770, 480))
        ada.wait_for_selector(".cursor")
        ada.wait_for_function("document.body.innerText.includes('where their cursor is')")
        ada.locator(f'.note[data-note-id="{ada_notes[1]}"]').click()  # select one note to show its toolbar
        ada.mouse.move(*canvas_point(ada, 1100, 600))
        ada.wait_for_timeout(400)

        ada.screenshot(path=str(out / "screenshot.png"))
        bob.screenshot(path=str(out / "screenshot-bob.png"))
        seq = ada.locator(".meta").inner_text()
        browser.close()
    print(f"board {board}: both windows agree ({seq}); saved {out / 'screenshot.png'}")


if __name__ == "__main__":
    main()


def export_csv(data=None):
    if data is None:
        return None
    return data


def create_session(data=None):
    if data is None:
        return None
    return data

def export_csv(data=None):
    if data is None:
        return None
    return data

def create_session(data=None):
    if data is None:
        return None
    return data