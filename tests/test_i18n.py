import os
import re

from dixo.i18n import MESSAGES

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_RE = re.compile(r"""\b(?:t|tr|dixoT)\(\s*['"]([a-z0-9_.]+)['"]""")


def test_all_template_keys_exist():
    missing = set()
    for dirpath, _, files in os.walk(os.path.join(ROOT, "templates")):
        for f in files:
            if f.endswith(".html"):
                text = open(os.path.join(dirpath, f), encoding="utf-8").read()
                missing |= {k for k in KEY_RE.findall(text) if k not in MESSAGES}
    assert not missing, sorted(missing)


def test_every_key_has_both_languages():
    assert all(zh and en for zh, en in MESSAGES.values())
