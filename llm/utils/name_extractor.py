import re
from dataclasses import dataclass, field
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Stop-words
# ---------------------------------------------------------------------------
STOP_WORDS = [
    "WEB-DL","WEBRip","BluRay","BDRip","BRRip","HDRip",
    "CAMRip","DVDRip","HDTV","HDTVRip","VODRip","VHSRip",
    "REMUX","HC.HDRip","HDCAM","HDTS","HQCAM","CAM","DVDSCR","SCR","R5",
    "480p","576p","720p","1080p","2160p","4K","8K",
    "x264","x265","H264","H265","HEVC","XviD","DivX",
    "AAC","AC3","DTS","DD5","Atmos","TrueHD",
    "10bit","HDR","HDR10","DV","DoVi","MKV","MP4","AVI",
    "Netflix","Amazon","Hotstar","ZEE5","SonyLiv",
    "Hoichoi","Chorki","JioHotstar","Disney",
    "NF","AMZN","DSNP","HMAX","PCOK","ATVP",
    "Hindi","Tamil","Telugu","Bengali","Punjabi",
    "Korean","Japanese","Chinese","Gujarati","Marathi","Malayalam","Kannada","Urdu",
    "Dubbed","Dual","ORG","ESub","HardSub","SoftSub",
    "REPACK","PROPER","UNRATED","THEATRICAL","DIRECTORS.CUT","V2","V3",
    "GDrive","CineFreak","TorrentCounter","Filmywap","MoviesBaba","Filmyzilla",
    "GB","MB", "18+", "[18+]", "Copy"
]

STOP_WORDS_SET: set = {w.lower() for w in STOP_WORDS}
STOP_PATTERN = re.compile(
    r'\b(' + "|".join(re.escape(w) for w in STOP_WORDS) + r')\b', re.I
)

# ---------------------------------------------------------------------------
# Edition bracket keywords — [Final Cut], [Extended Edition] etc.
# Only brackets whose content matches these words are removed.
# Brackets like [Advent Children] are preserved.
# ---------------------------------------------------------------------------
EDITION_KEYWORDS = re.compile(
    r'\b(cut|edition|version|extended|director|theatrical|unrated|'
    r'remastered|restored|special|anniversary|ultimate|collectors?|'
    r'definitive|complete|final|skynet|abridged)\b',
    re.I,
)

# ---------------------------------------------------------------------------
# Year patterns
# ---------------------------------------------------------------------------
YEAR_BRACKET = re.compile(r'\((19|20)\d{2}\)')
YEAR_RANGE   = re.compile(r'\((19|20)\d{2}[-–](19|20)\d{2}\)')
YEAR_BARE    = re.compile(r'\b(19|20)\d{2}\b')

MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)
DATE_PATTERN = re.compile(
    rf'\b\d{{1,2}}(st|nd|rd|th)?\s+({MONTHS})\s+(19|20)\d{{2}}\b'
    rf'|\b({MONTHS})\s+(19|20)\d{{2}}\b',
    re.I,
)

# ---------------------------------------------------------------------------
# Season / episode patterns
# ---------------------------------------------------------------------------
SEASON_EPISODE_PATTERNS = [
    re.compile(r'\bSeason\s?\d+\s+Episode\s?\d+\b',         re.I),  # TV listing
    re.compile(r'\bS\d{1,2}E\d{1,4}\s*[-–]\s*(S\d{1,2})?E\d{1,4}\b', re.I),
    re.compile(r'\bS\d{1,2}E\d{1,4}\b', re.I),
    re.compile(r'S\d{1,2}\s*[-–]\s*S\d{1,2}\b',             re.I),
    re.compile(r'\bS\d{1,2}\b',                              re.I),
    re.compile(r'\bSeason\s?\d+\b',                          re.I),
    re.compile(r'\bEpisode\s?\d+\b',                         re.I),
    re.compile(r'\bEp\.?\s?\d+\b',                           re.I),
    re.compile(r'\[S\d+\s*Ep',                               re.I),
    re.compile(r'\[Ep\d+',                                   re.I),
    re.compile(r'S\d+\s*\|\s*Ep',                           re.I),
    re.compile(r'\b\d{1,2}x\d{2}\b'),
    re.compile(r'\bComplete\s+Series\b',                     re.I),
    re.compile(r'\bMini.?Series\b',                          re.I),
]

# Content-type tags — kept SEPARATE from season_tag
ANIME_PATTERNS  = [re.compile(p, re.I) for p in (r'\bOVA\b', r'\bONA\b', r'\bOAD\b')]
KDRAMA_PATTERNS = [re.compile(p, re.I) for p in (r'\bK-?Drama\b', r'\bKDrama\b')]

LANG_TAG_PATTERN = re.compile(r'\[[A-Za-z]{2,8}-[A-Za-z]{2,8}\]', re.I)

# Common acronyms to restore dots after dot→space conversion
ACRONYMS = {'swat', 'fbi', 'cia', 'nsa', 'usa', 'uk', 'us', 'nypd', 'lapd',
             'ncis', 'csi', 'atf', 'dea', 'mi6', 'mi5', 'cia', 'nato'}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------
@dataclass
class TitleInfo:
    title:       str
    year:        Optional[str] = None   # "2008" or "2001-2003"
    season_tag:  Optional[str] = None   # "S01E05", "Season 2", …
    content_tag: Optional[str] = None   # "OVA", "K-Drama", …
    raw:         str           = ""

    _TV_LISTING_RE   = re.compile(r'^Season\s?\d+\s+Episode\s?\d+$', re.I)
    _FULL_SERIES_RE  = re.compile(r'^S\d{1,2}[-–]S\d{1,2}$',        re.I)

    def __str__(self) -> str:
        parts = [self.title]
        # Content tag (OVA / K-Drama) always shown when no year
        if self.content_tag and not self.year:
            parts.append(self.content_tag)
        # Season tag: drop when year present, or when it's a TV listing / full-series range
        # Drop season_tag when:
        # a) year is known (Title + Year is enough)
        # b) it's a TV listing like "Season 1 Episode 1" (always redundant)
        # c) it's a full-series range (S01-S05) AND year is also present
        _full_series = self._FULL_SERIES_RE.match(self.season_tag) if self.season_tag else False
        keep_season = (
            self.season_tag
            and not self.year
            and not self._TV_LISTING_RE.match(self.season_tag)
        ) or (
            self.season_tag
            and not self.year
            and bool(_full_series)
        )
        # Simplify: keep when no year AND not TV-listing; full-series range kept when no year
        keep_season = (
            bool(self.season_tag)
            and not self.year
            and not self._TV_LISTING_RE.match(self.season_tag)
        )
        if keep_season:
            parts.append(self.season_tag)
        if self.year:
            parts.append(f"({self.year})")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stop_word_ratio(text: str) -> float:
    words = re.sub(r"[^\w\s]", " ", text).split()
    if not words:
        return 0.0
    return sum(1 for w in words if w.lower() in STOP_WORDS_SET) / len(words)


def _starts_like_title(segment: str) -> bool:
    """True when segment starts with a capitalized word → likely a real subtitle."""
    seg = segment.strip()
    if not seg:
        return False
    first_word = seg.split()[0]
    return bool(re.match(r'^[A-Z]', first_word))


def _should_drop_right(segment: str) -> bool:
    """
    True when a pipe/dash right segment is clearly metadata.
    Preserves segments that start with a Capital word (subtitle heuristic).
    """
    seg = segment.strip()
    if not seg:
        return True
    # Bare year → drop (will be rescued separately)
    if YEAR_BARE.fullmatch(seg):
        return True
    # Starts with stop-word → metadata
    if STOP_PATTERN.match(seg):
        return True
    # High stop-word density → metadata
    if _stop_word_ratio(seg) >= 0.5:
        return True
    # Starts with a capitalized word → probably subtitle, keep it
    if _starts_like_title(seg):
        return False
    return False


def _normalize_separators(text: str) -> str:
    """Replace only em-dash (—). Preserve en-dash (–) used in titles."""
    return text.replace("\u2014", " - ")


def _is_dot_format(text: str) -> bool:
    """
    Detect dot-separated scene releases.
    Works correctly for:
      - Normal: The.Dark.Knight.2008.1080p.BluRay
      - Abbreviation: S.W.A.T.2003.1080p  (single-char ratio > 0.4 → NOT dot-format)
      - Short title: 1917.2019.1080p.BluRay
    """
    stripped = text.split('(')[0].strip()
    if '.' not in stripped or ' ' in stripped:
        return False
    tokens = stripped.split('.')
    if len(tokens) < 3:
        return False
    # Mostly single-char tokens → abbreviation, not dot-format
    single_char = sum(1 for t in tokens if len(t) == 1)
    if single_char / len(tokens) > 0.4:
        return False
    meta_hits = sum(
        1 for t in tokens[1:]
        if t.lower() in STOP_WORDS_SET or YEAR_BARE.fullmatch(t)
    )
    return meta_hits >= 1


def _restore_dot_abbreviations(title: str) -> str:
    """
    Restore dots in abbreviations after dot→space conversion.
    Handles common honorifics and known acronyms.
    """
    # Honorifics
    for abbr in ('Dr', 'Mr', 'Mrs', 'Ms', 'St', 'Jr', 'Sr', 'Prof'):
        title = re.sub(rf'\b{abbr}\b(?!\.)', f'{abbr}.', title)
    # Known acronyms: if consecutive single-letter tokens spell a known acronym
    # e.g. "S W A T" → "S.W.A.T."
    def _join_acronym(m):
        letters = m.group(0).replace(' ', '')
        if letters.lower() in ACRONYMS:
            return '.'.join(list(letters)) + '.'
        return m.group(0)
    title = re.sub(r'\b([A-Z] ){2,}[A-Z]\b', _join_acronym, title)
    return title


def _extract_year_info(text: str) -> Tuple[Optional[str], str]:
    """
    Returns (year_display, text_before_year).
    Strategy for multiple bare years:
      - If two years are adjacent with no content between → title + release year
      - Otherwise pick the year that sits just before the first metadata token
    """
    # Bracketed range wins
    m = YEAR_RANGE.search(text)
    if m:
        return m.group().strip("()"), text[:m.start()]

    # Bracketed year wins over bare
    m = YEAR_BRACKET.search(text)
    if m:
        return m.group().strip("()"), text[:m.start()]

    all_years = list(YEAR_BARE.finditer(text))
    if not all_years:
        return None, text

    # Two adjacent years → first is title number, second is release year
    if len(all_years) >= 2:
        y1, y2 = all_years[0], all_years[1]
        between = text[y1.end():y2.start()].strip()
        if not between or between in ('.', ','):
            before_y1 = text[:y1.start()].strip()
            if before_y1:
                # There's real title content before y1 → y1 is likely a release year
                # Use y1 as release year instead of y2, and y2 as confirmation
                # Actually: pick y2 as release year, title is just before y1
                return y2.group(), text[:y1.start()].rstrip()
            else:
                # Nothing before y1 → y1 IS the title (e.g. "1917")
                return y2.group(), y1.group()

    # Multiple non-adjacent years: pick the one just before first stop-word/metadata
    if len(all_years) >= 2:
        first_stop = STOP_PATTERN.search(text)
        if first_stop:
            # Use the last year that starts before the first stop-word
            candidates = [y for y in all_years if y.start() < first_stop.start()]
            if candidates:
                m = candidates[-1]
                return m.group(), text[:m.start()]

    m = all_years[0]
    return m.group(), text[:m.start()]


def _extract_season_and_content(text: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Returns (cut_position, season_tag, content_tag).
    season_tag: S01E05, Season 2, etc.
    content_tag: OVA, K-Drama, etc. (separate field)
    Both require non-trivial title content before them.
    """
    # Find content tags (OVA / K-Drama) — separate from season
    content_tag = None
    content_pos = None
    for pattern in ANIME_PATTERNS + KDRAMA_PATTERNS:
        m = pattern.search(text)
        if m:
            title_before = re.sub(r'[\s\-–(,]+$', '', text[:m.start()]).strip()
            if title_before:
                content_pos = m.start()
                content_tag = m.group()
                break

    # Find season tags
    season_tag = None
    season_pos = None
    for pattern in SEASON_EPISODE_PATTERNS:
        for m in pattern.finditer(text):
            title_before = re.sub(r'[\s\-–(,]+$', '', text[:m.start()]).strip()
            if title_before:
                if season_pos is None or m.start() < season_pos:
                    season_pos = m.start()

    if season_pos is not None:
        # Pick the longest match at that position
        all_matches = []
        for pattern in SEASON_EPISODE_PATTERNS:
            for m in pattern.finditer(text):
                if m.start() == season_pos:
                    all_matches.append(m)
        if all_matches:
            best = max(all_matches, key=lambda m: len(m.group()))
            season_tag = best.group()

    # Cut position: earliest of season/content
    cut_pos = None
    if season_pos is not None:
        cut_pos = season_pos
    if content_pos is not None:
        cut_pos = content_pos if cut_pos is None else min(cut_pos, content_pos)

    return cut_pos, season_tag, content_tag


def _smart_split_pipe(text: str) -> str:
    parts = [p.strip() for p in text.split("|")]
    if len(parts) == 1:
        return text
    kept = [parts[0]]
    found_year = None
    for seg in parts[1:]:
        if _should_drop_right(seg):
            if not found_year and YEAR_BARE.fullmatch(seg.strip()):
                found_year = seg.strip()
            break
        kept.append(seg)
    result = " | ".join(kept) if len(kept) > 1 else kept[0]
    if found_year and f"({found_year})" not in result and found_year not in result:
        result = f"{result} ({found_year})"
    return result


def _smart_split_dash(text: str) -> str:
    """
    Split on ' - '.
    Drop right side only when it is clearly metadata (lowercase start or stop-words).
    Preserve subtitles that start with a Capital word.
    Rescue bare year before dropping.
    """
    parts = re.split(r' - ', text)
    if len(parts) == 1:
        return text
    result_parts = [parts[0]]
    found_year = None
    for seg in parts[1:]:
        if _should_drop_right(seg):
            if not found_year and YEAR_BARE.fullmatch(seg.strip()):
                found_year = seg.strip()
            break
        result_parts.append(seg)
    result = " - ".join(result_parts)
    if found_year and f"({found_year})" not in result and found_year not in result:
        result = f"{result} ({found_year})"
    return result


def _remove_lang_tags(text: str) -> str:
    return LANG_TAG_PATTERN.sub('', text)


def _remove_edition_brackets(text: str) -> str:
    """
    Remove [] brackets that contain edition keywords (Final Cut, Extended, etc.).
    Preserve brackets whose content doesn't match — e.g. [Advent Children].
    """
    def replacer(m):
        inner = m.group(1)
        if EDITION_KEYWORDS.search(inner):
            return ''
        return m.group(0)
    return re.sub(r'\s*\[([^\]]{2,50})\]', replacer, text)


def _clean_title(title: str) -> str:
    title = re.sub(r'[\s\-–(,]+$', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip()


def _dot_format_extract(text: str) -> Optional[TitleInfo]:
    """Fast-path for dot-separated scene releases."""
    if not _is_dot_format(text):
        return None

    spaced = text.replace('.', ' ')
    spaced = _restore_dot_abbreviations(spaced)
    spaced = _remove_lang_tags(spaced)
    spaced = _remove_edition_brackets(spaced)
    spaced = re.sub(r'\[.*?\]', '', spaced)   # remove any remaining [] blocks
    spaced = re.sub(r'\s+', ' ', spaced).strip()

    year, before = _extract_year_info(spaced)
    search_in = before if year else spaced
    cut_pos, s_tag, c_tag = _extract_season_and_content(search_in)

    if cut_pos is not None:
        raw = search_in[:cut_pos]
    elif year and before.strip():
        raw = before
    else:
        raw = STOP_PATTERN.split(spaced)[0]

    # Apply stop-word split to title portion too (catches leftover tokens like "BluRay")
    raw = STOP_PATTERN.split(raw)[0]
    title = _clean_title(raw)

    if not title:
        return None
    return TitleInfo(title=title, year=year, season_tag=s_tag, content_tag=c_tag, raw=text)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_title_info(text: str) -> TitleInfo:
    original = text

    # 1. Dot-format fast path
    result = _dot_format_extract(text)
    if result:
        return result

    # 2. Normalize
    text = _normalize_separators(text)
    text = _remove_lang_tags(text)
    text = _remove_edition_brackets(text)

    # 3. Smart pipe / dash split
    text = _smart_split_pipe(text)
    text = _smart_split_dash(text)

    # 4. Remove remaining braced blocks and dates
    text = re.sub(r'\{.*?\}', '', text)
    text = DATE_PATTERN.sub('', text)

    # 5. Stop-word split
    text = STOP_PATTERN.split(text)[0]
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[\s\-–(]+$', '', text).strip()

    # 6. Year extraction
    year, before_year = _extract_year_info(text)

    # 7. Season / content extraction
    search_in = before_year if year else text
    cut_pos, s_tag, c_tag = _extract_season_and_content(search_in)

    if cut_pos is not None:
        title = _clean_title(search_in[:cut_pos])
    else:
        raw = before_year.strip() if year else text.strip()
        title = _clean_title(raw) or _clean_title(text)

    return TitleInfo(title=title, year=year, season_tag=s_tag, content_tag=c_tag, raw=original)


def extract_title(text: str) -> str:
    return str(extract_title_info(text))