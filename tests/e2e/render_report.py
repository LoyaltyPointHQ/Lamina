"""Standalone Polish HTML report: uv run render_report.py results.xml --output e2e-report.html.

Uses only Python's standard library. XML test cases, not a fixed expected count,
are the source of displayed results; suite counters are checked independently.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import re
import xml.etree.ElementTree as ET


PROFILES = {
    "fs-inline": ("Filesystem", "Filesystem / Inline"),
    "fs-separate": ("Filesystem", "Filesystem / SeparateDirectory"),
    "fs-xattr": ("Filesystem", "Filesystem / Xattr"),
    "fs-memory": ("Filesystem", "InMemory"),
    "fs-sqlite": ("Filesystem", "SQL / SQLite"),
    "memory-memory": ("InMemory", "InMemory"),
    "memory-inline": ("InMemory", "Filesystem / Inline"),
    "memory-separate": ("InMemory", "Filesystem / SeparateDirectory"),
    "memory-sqlite": ("InMemory", "SQL / SQLite"),
    "fs-postgres": ("Filesystem", "SQL / PostgreSQL"),
    "memory-postgres": ("InMemory", "SQL / PostgreSQL"),
}
STATUS = {"passed": "OK", "failed": "FAIL", "error": "ERROR", "skipped": "POMINIĘTY"}
SCOPE = {
    "test_auth": "Publiczny healthcheck, odmowa anonimowego GET i złego podpisu SigV4; "
    "odczyt/listowanie dla read-only oraz odmowa zapisu, kasowania, tagowania i inicjacji multipart.",
    "test_clients": "boto3, AWS CLI, rclone i MinIO mc: interoperacyjność upload/download "
    "z porównaniem bajtów, pliki binarne/puste i nazwy Unicode; CLI multipart, "
    "copy/move/delete, synchronizacja w obie strony z usuwaniem i ochroną obcego prefiksu; "
    "bucket create/list/remove; AWS paginacja, metadane i SHA256.",
    "test_directory": "Rozszerzenie Laminy Directory: nagłówki HEAD, stabilna paginacja V2, "
    "wspólny limit obiektów i prefiksów, błędne argumenty i nieprzezroczysty marker d1: w V1.",
    "test_s3_api": "Bucket CRUD i błędy, obiekty CRUD/overwrite, ETag MD5, zakresy bajtów, "
    "warunkowy GET/HEAD, listowanie V1/V2, delimiter, UTF-8, start-after, batch delete; "
    "copy między bucketami, metadane i tagi, konfiguracja lifecycle i walidacja; "
    "CRC32/SHA1/SHA256 na GET/HEAD oraz po CopyObject i błędny Content-MD5; multipart, list-parts, abort, "
    "UploadPartCopy całego obiektu i zakresu bajtów, "
    "błędny ETag części; presigned PUT/GET; surowe bajty PutObject i UploadPart dla "
    "urlencoded (także charset), multipart/form-data i octet-stream; "
    "równoległe niezależne obiekty.",
    "test_storage": "Zmiany plików poza API i odświeżanie metadanych oraz SHA256/ETag "
    "na powtórnych GET/HEAD z zachowaniem tagów i metadanych użytkownika, ukrywanie plików "
    "wewnętrznych, kasowanie bucketa z pustymi katalogami; restart procesu i trwałość "
    "danych, checksum, metadanych, tagów, lifecycle oraz niedokończonego multipart. "
    "Przypadki wymagające dysku/trwałości są jawnie pomijane w profilach ulotnych.",
    "test_signed_streaming": "Signed aws-chunked przez przypięty MinIO mc: HTTP + SigV4, "
    "bez --checksum. Single PUT z --disable-multipart: pusty obiekt, 1 bajt, granice "
    "64 KiB, wiele chunków oraz duży plik; osobno podpisywane części multipart. "
    "Kontrola długości, dokładnego ETag i pełnych bajtów. Tryb wynika z konfiguracji "
    "i kodu przypiętego klienta, bez przechwytywania ruchu. Nie obejmuje signed "
    "trailer ani celowo uszkodzonych podpisów chunków.",
}
CSS = """
:root{font-family:system-ui,sans-serif;color:#172033;background:#f4f6fa;color-scheme:light}
body{max-width:1450px;margin:auto;padding:1.5rem}h1{margin-bottom:.4rem}
section{background:white;padding:1.2rem;margin:1rem 0;border:1px solid #d4dce8;border-radius:8px}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:.65rem;border-bottom:1px solid #d4dce8;vertical-align:top}
th{background:#edf1f7}caption{text-align:left;font-weight:600;padding:.5rem 0}
.scroll{overflow:auto}.badge{font-weight:700;white-space:nowrap}
.passed{color:#14613a}.failed,.error{color:#a71d26}.skipped{color:#755200}
.warning{border-left:5px solid #a71d26;padding:1rem;background:#fff1f0}
pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f1f3f7;padding:1rem}
code,td{overflow-wrap:anywhere}summary{cursor:pointer;padding:.5rem 0}
label{display:inline-block;margin:.4rem 1rem .4rem 0}input,select{font:inherit;padding:.4rem;max-width:100%}
.cards{display:flex;flex-wrap:wrap;gap:1rem}.cards div{background:#edf1f7;padding:1rem;border-radius:6px}
.matrix-scroll{max-height:75vh;border:1px solid #d4dce8}
#cases{width:max-content;border-collapse:separate;border-spacing:0}
#cases th,#cases td{min-width:8rem;max-width:10rem;border-right:1px solid #d4dce8}
#cases thead th{position:sticky;top:0;z-index:2;vertical-align:bottom}
#cases thead th:first-child{z-index:3}
#cases th:first-child{position:sticky;left:0;background:#edf1f7;min-width:22rem;max-width:24rem}
#cases tbody th{z-index:1}
#cases .category-row th{position:static;max-width:none;background:#dbe5f4;color:#233b61}
#cases .category-row span{position:sticky;left:.65rem}
#cases td{text-align:center;background:#fff}
#cases code{font-size:.8rem;white-space:normal;overflow-wrap:anywhere}
#cases small{display:block;margin:.35rem 0;color:#526078}
.case-result{padding:.4rem}.case-result time{display:block;margin-top:.3rem}
.case-result.dimmed{opacity:.4}.missing{color:#697386}
#cases a.badge{color:inherit;text-decoration:underline;text-underline-offset:.2rem}
a{color:#174c96} [hidden]{display:none!important}
@media print{.filters{display:none}body{padding:0}section{break-inside:avoid}}
"""
JS = """
const rows = [...document.querySelectorAll('#cases tbody tr[data-test]')];
const headers = [...document.querySelectorAll('#cases thead th[data-column]')];
const groups = [...document.querySelectorAll('#cases tbody[data-category]')];
function filterCases() {
 const query = document.getElementById('query').value.toLocaleLowerCase();
 const profile = document.getElementById('profile').value;
 const status = document.getElementById('status').value;
 const category = document.getElementById('category').value;
 for (const header of headers)
  header.hidden = !!profile && header.dataset.profile !== profile;
 let tests = 0;
 let visible = 0;
 for (const row of rows) {
  const cells = headers.map(header => row.querySelector(`[data-column="${header.dataset.column}"]`));
  const hasStatus = !status || cells.some((cell, i) => !headers[i].hidden
    && [...cell.querySelectorAll('[data-status]')].some(result => result.dataset.status === status));
  const show = row.querySelector('th').textContent.toLocaleLowerCase().includes(query) && hasStatus
    && (!category || row.parentElement.dataset.category === category);
  row.hidden = !show;
  if (show) tests++;
  for (let i = 0; i < cells.length; i++) {
   cells[i].hidden = headers[i].hidden;
   for (const result of cells[i].querySelectorAll('[data-status]')) {
    const matches = !status || result.dataset.status === status;
    result.classList.toggle('dimmed', !matches);
    if (show && !headers[i].hidden && matches) visible++;
   }
  }
 }
 for (const group of groups) {
  group.hidden = ![...group.querySelectorAll('tr[data-test]')].some(row => !row.hidden);
  group.querySelector('.category-row th').colSpan = headers.filter(header => !header.hidden).length + 1;
 }
 document.getElementById('visible').textContent =
   `Profile: ${headers.filter(header => !header.hidden).length} / ${headers.length}; `
   + `testy: ${tests} / ${rows.length}; pasujące wyniki: ${visible}`;
}
function revealTarget() {
 const target = document.getElementById(location.hash.slice(1));
 if (!target) return;
 for (let node = target; node; node = node.parentElement)
  if (node.tagName === 'DETAILS') node.open = true;
 target.scrollIntoView({block: 'nearest', inline: 'nearest'});
}
for (const id of ['query', 'profile', 'status', 'category'])
 document.getElementById(id).addEventListener('input', filterCases);
window.addEventListener('hashchange', revealTarget);
filterCases();
revealTarget();
"""


def number(value):
    return float(value or 0)


def read_cases(root):
    cases = []
    for node in root.iter("testcase"):
        name = node.get("name", "(bez nazwy)")
        profile = next(
            (p for p in PROFILES if re.search(r"(?:\[|-)" + re.escape(p) + r"(?:-|\])", name)),
            "nieznany",
        )
        status = next(
            (
                s
                for tag, s in (("error", "error"), ("failure", "failed"), ("skipped", "skipped"))
                if node.find(tag) is not None
            ),
            "passed",
        )
        details = []
        for child in node:
            if child.tag in {"failure", "error", "skipped"}:
                details.append(
                    f"{child.tag}: {child.get('message', '')}\n{''.join(child.itertext())}".strip()
                )
        cases.append(
            {
                "name": name,
                "module": node.get("classname", "(bez klasy)"),
                "profile": profile,
                "status": status,
                "time": number(node.get("time")),
                "details": "\n\n".join(details),
            }
        )
    return cases


def counts(cases):
    return Counter(case["status"] for case in cases)


def count_cells(cases):
    totals = counts(cases)
    return f"<td>{len(cases)}</td>" + "".join(f"<td>{totals[s]}</td>" for s in STATUS)


def case_column(case):
    """Strip only the storage parameter; retain every other scenario parameter."""
    name = case["name"]
    profile = re.escape(case["profile"])
    name = re.sub(r"\[" + profile + r"\]", "", name, count=1)
    name = re.sub(r"\[" + profile + "-", "[", name, count=1)
    return case["module"], name


def test_category(test):
    module, name = test
    module = module.split(".")[-1]
    if module == "test_auth":
        return 0, "Autoryzacja i healthcheck"
    if module == "test_clients":
        return 9, "Klienci CLI i interoperacyjność"
    if module == "test_directory":
        return 10, "Buckety Directory"
    if module == "test_storage":
        return 11, "Filesystem, trwałość i restart"
    if module == "test_signed_streaming":
        return 12, "Signed streaming (aws-chunked)"
    if module != "test_s3_api":
        return 13, "Pozostałe testy"
    function = name.split("[", 1)[0]
    if function in {"test_bucket_lifecycle", "test_nonempty_bucket_cannot_be_deleted"}:
        return 1, "Operacje na bucketach"
    if "lifecycle" in function:
        return 5, "Konfiguracja lifecycle"
    if function in {"test_object_tagging", "test_invalid_tags_rejected"}:
        return 4, "Tagowanie obiektów"
    if "multipart" in function:
        return 7, "Multipart upload"
    if function.startswith(("test_list_", "test_listing_")):
        return 3, "Listowanie i paginacja"
    if "checksum" in function or "md5" in function:
        return 6, "Checksumy i integralność danych"
    if "presigned" in function or "form_content_type" in function:
        return 8, "Presigned URL i typ zawartości"
    return 2, "Obiekty, metadane i kopiowanie"


def render_case_matrix(cases):
    tests = sorted({case_column(case) for case in cases}, key=lambda t: (test_category(t), t))
    categories = sorted({test_category(test) for test in tests})
    category_counts = Counter(test_category(test) for test in tests)
    profiles = [p for p in PROFILES if any(c["profile"] == p for c in cases)]
    profiles += sorted({c["profile"] for c in cases} - set(PROFILES))
    indexed = {}
    for index, case in enumerate(cases):
        indexed.setdefault((case["profile"], case_column(case)), []).append((index, case))
    parts = [
        '<section id="case-matrix"><h2>Przypadki × profile</h2>',
        "<p>Wiersze: testy pogrupowane w kategorie, wraz z wariantami parametrów. "
        "Kolumny: profile storage. "
        "Komórka: status i czas. Kliknij status niepowodzenia lub pominięcia, aby zobaczyć szczegóły. "
        "Tabela jest przewijana; nagłówki profili i nazwy testów pozostają widoczne.</p>",
        '<div class="filters"><label>Szukaj testu <input id="query" type="search"></label>',
        '<label>Kategoria <select id="category"><option value="">Wszystkie</option>',
    ]
    parts += [
        f'<option value="category-{index}">{escape(label)}</option>' for index, label in categories
    ]
    parts.append(
        '</select></label><label>Profil <select id="profile"><option value="">Wszystkie</option>'
    )
    parts += [f'<option value="{escape(p)}">{escape(p)}</option>' for p in profiles]
    parts.append(
        '</select></label><label>Testy ze statusem <select id="status">'
        '<option value="">Wszystkie</option>'
    )
    parts += [f'<option value="{s}">{label}</option>' for s, label in STATUS.items()]
    parts += [
        "</select></label><p>Filtr statusu wybiera wiersze z pasującym wynikiem; "
        "pozostałe wyniki pozostają widoczne, przygaszone, do porównania.</p>",
        '<p id="visible" role="status" aria-live="polite"></p></div>',
        '<div class="scroll matrix-scroll" tabindex="0" role="region" '
        'aria-label="Macierz wyników testów"><table id="cases">'
        "<caption>Status i czas dla każdego testu oraz profilu "
        '(bez JavaScript widoczne są wszystkie)</caption><thead><tr><th scope="col">Test</th>',
    ]
    for index, profile in enumerate(profiles):
        parts.append(
            f'<th scope="col" data-column="{index}" data-profile="{escape(profile)}">'
            f"{escape(profile)}</th>"
        )
    parts.append("</tr></thead>")
    current_category = None
    for module, name in tests:
        category = test_category((module, name))
        if category != current_category:
            if current_category is not None:
                parts.append("</tbody>")
            current_category = category
            index, label = category
            parts.append(
                f'<tbody data-category="category-{index}"><tr class="category-row">'
                f'<th scope="rowgroup" colspan="{len(profiles) + 1}"><span>{escape(label)}'
                f" · {category_counts[category]} testów</span></th></tr>"
            )
        parts.append(
            f'<tr data-test="{escape(name)}"><th scope="row"><small>{escape(module)}</small>'
            f"<code>{escape(name)}</code></th>"
        )
        for column, profile in enumerate(profiles):
            entries = indexed.get((profile, (module, name)), [])
            parts.append(f'<td data-column="{column}">')
            if not entries:
                parts.append('<span class="missing">—<br>Brak wyniku</span>')
            for index, case in entries:
                status = case["status"]
                label = STATUS[status]
                badge = (
                    f'<a class="badge" href="#detail-{index}">{label}</a>'
                    if case["details"]
                    else f'<span class="badge">{label}</span>'
                )
                parts.append(
                    f'<div id="case-{index}" class="case-result {status}" '
                    f'data-status="{status}">{badge}'
                    f'<time datetime="PT{case["time"]:.3f}S">{case["time"]:.3f} s</time></div>'
                )
            parts.append("</td>")
        parts.append("</tr>")
    if current_category is not None:
        parts.append("</tbody>")
    parts.append("</table></div></section>")
    return "\n".join(parts)


def render(source, command=""):
    root = ET.parse(source).getroot()
    cases = read_cases(root)
    totals = counts(cases)
    suites = [s for s in root.iter("testsuite") if not s.findall("testsuite")]
    declared = {
        key: sum(int(s.get(key, "0")) for s in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    observed = dict(
        tests=len(cases),
        failures=totals["failed"],
        errors=totals["error"],
        skipped=totals["skipped"],
    )
    complete_counters = bool(suites) and all(
        all(key in suite.attrib for key in declared) for suite in suites
    )
    consistent = complete_counters and declared == observed
    timestamps = sorted({s.get("timestamp") for s in suites if s.get("timestamp")})
    duration = sum(number(s.get("time")) for s in suites)
    result = (
        "Wykryto niepowodzenia — szczegóły poniżej."
        if totals["failed"] or totals["error"]
        else "Brak niepowodzeń w zapisanych przypadkach; pominięcia nie potwierdzają poprawności."
        if cases
        else "Brak wyników przypadków testowych — brak potwierdzenia poprawności."
    )
    parts = [
        '<!doctype html><html lang="pl"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>Lamina — raport E2E</title><style>{CSS}</style></head><body>",
        "<header><h1>Lamina — raport E2E</h1><p>Rzeczywisty proces aplikacji, HTTP i klienci "
        "boto3 / AWS CLI / rclone / MinIO mc.</p></header>",
        f'<p class="warning">{result}</p>',
        f"<p>Źródło: <code>{escape(str(source))}</code><br>",
        f"Znacznik czasu suite z XML: {escape(', '.join(timestamps) or 'brak')}<br>",
        f"Wygenerowano: {datetime.now(timezone.utc).isoformat()} (UTC)</p>",
        f"<p>Polecenie uruchomienia (przekazane do raportu): <code>{escape(command or 'nie podano')}</code></p>",
        '<section><h2>Wyniki zapisane w JUnit</h2><div class="cards">',
        f"<div>Wpisy testcase: <strong>{len(cases)}</strong></div>",
    ]
    parts += [
        f'<div class="{s}">{label}: <strong>{totals[s]}</strong></div>'
        for s, label in STATUS.items()
    ]
    parts += [
        f"<div>Łączny czas suite: <strong>{duration:.2f} s</strong></div></div>",
        "<p>Dla wielu przebiegów jest to suma ich czasów, nie czas ścienny pracy równoległej.</p>",
        f"<p>Suma czasów testcase: {sum(c['time'] for c in cases):.2f} s. "
        "Czas suite obejmuje też narzut uruchomienia.</p>",
        f"<p>Liczniki suite: {escape(str(declared))}; przeliczone testcase: {escape(str(observed))}. "
        f"<strong>{'Zgodne.' if consistent else 'Niezgodne lub brak kompletnych liczników suite.'}</strong></p>",
        "<p>Liczymy wpisy XML, nie unikalne funkcje: pytest może zapisać osobny wpis błędu "
        "teardown. Wpis z error ma pierwszeństwo nad failure i skipped. XML nie dowodzi "
        "kompletności planowanego uruchomienia; brak profilu oznacza brak wyników, nie sukces.</p></section>",
        '<section><h2>Macierz storage</h2><div class="scroll"><table><caption>Konfiguracje i wyniki</caption>',
        "<thead><tr><th>Profil</th><th>Dane</th><th>Metadane</th><th>Wpisy</th>"
        "<th>OK</th><th>FAIL</th><th>ERROR</th><th>Pominięte</th></tr></thead><tbody>",
    ]
    for profile, (data, metadata) in {
        **PROFILES,
        **({"nieznany": ("?", "?")} if any(c["profile"] == "nieznany" for c in cases) else {}),
    }.items():
        selected = [c for c in cases if c["profile"] == profile]
        parts.append(
            f"<tr><th>{profile}{'' if selected else ' — brak wyników'}</th>"
            f"<td>{data}</td><td>{metadata}</td>{count_cells(selected)}</tr>"
        )
    parts += [
        "</tbody></table></div><p>SQL przechowuje metadane, nie dane obiektów. "
        "InMemory + Xattr nie jest prawidłową kombinacją: Xattr wymaga fizycznych plików. "
        "Profil fs-xattr wymaga rzeczywistej próby zapisu/odczytu user.* w /tmp.</p></section>",
        "<section><h2>Co jest testowane</h2><p>Opis zakresu zestawu; wyniki poniżej określają, "
        "które przypadki faktycznie zapisano i z jakim rezultatem.</p><dl>",
    ]
    for module, description in SCOPE.items():
        selected = [c for c in cases if c["module"].split(".")[-1] == module]
        parts.append(
            f"<dt><strong>{module}.py</strong> — {len(selected)} wpisów</dt><dd>{description}</dd>"
        )
    parts += [
        "</dl><h3>Poza zakresem</h3><p>Redis / blokady rozproszone, wiele replik, NFS/CIFS, "
        "TLS/HTTP2, wdrożenie i Helm, awarie sieci oraz długotrwałe duże obciążenie. "
        "Zadania tła cleanup i lifecycle expiration są wyłączone: badamy konfigurację "
        "lifecycle, nie czasowe wygasanie. Directory oznacza rozszerzenie Laminy, "
        "nie pełny AWS S3 Express / CreateSession. Nie jest to certyfikacja całego S3.</p>"
        "<h3>Izolacja</h3><p>Harness tworzy własny storage i pliki klientów pod "
        "/tmp/lamina-e2e-* i sprząta je w finalizerach; PostgreSQL jest jednorazowym "
        "kontenerem. Sam XML nie stanowi niezależnego dowodu posprzątania hosta.</p>"
        "<h3>Wersje i odtwarzalność</h3><p>Wersje zależności są przypięte w "
        "<code>tests/e2e/pyproject.toml</code>, <code>tests/e2e/uv.lock</code> oraz "
        "<code>tests/e2e/install-clients.sh</code>. XML nie potwierdza wersji "
        "faktycznie zainstalowanych programów; instrukcje i granice zestawu opisuje "
        "<code>tests/e2e/README.md</code>.</p></section>",
        "<section><h2>Niepowodzenia i powody pominięć</h2>",
    ]
    for status in ("failed", "error", "skipped"):
        selected = [(i, c) for i, c in enumerate(cases) if c["status"] == status]
        parts.append(
            f'<details><summary class="{status}">{STATUS[status]}: {len(selected)}</summary>'
        )
        for i, case in selected:
            parts.append(
                f'<details id="detail-{i}"><summary><a href="#case-{i}">{escape(case["profile"])}</a> '
                f"{escape(case['name'])}</summary><pre>{escape(case['details'])}</pre></details>"
            )
        parts.append("</details>")
    parts.append("</section>")
    parts.append(render_case_matrix(cases))
    parts.append(f"<script>{JS}</script></body></html>")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit", type=Path, help="Plik JUnit XML z rzeczywistego uruchomienia")
    parser.add_argument("--output", type=Path, default=Path("e2e-report.html"))
    parser.add_argument("--command", default="", help="Dokładne polecenie uruchomienia testów")
    args = parser.parse_args()
    if args.output.resolve() == args.junit.resolve():
        parser.error("Plik wyjściowy nie może zastąpić źródłowego XML")
    try:
        report = render(args.junit, args.command)
        args.output.write_text(report, encoding="utf-8")
    except (OSError, ET.ParseError, ValueError) as error:
        parser.exit(2, f"Nie można wygenerować raportu: {error}\n")
    print(f"Raport: {args.output}")


if __name__ == "__main__":
    main()
