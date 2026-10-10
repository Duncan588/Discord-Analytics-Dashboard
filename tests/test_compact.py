
from dixo.core import compact_number

def test_compact():
    assert compact_number(100) == "100"
    assert compact_number(1200) == "1.2K"
    assert compact_number(2000) == "2K"
    assert compact_number(5_000_000) == "5M"
    assert compact_number(1_000_000_000) == "1B"
    assert compact_number(999) == "999"
