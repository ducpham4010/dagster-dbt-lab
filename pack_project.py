"""
pack_project.py — "Nén" / "giải nén" source code của project qua 1 file .txt.

PACK ("nén"):
    - Scan đệ quy từ thư mục gốc (mặc định: thư mục hiện tại) trở xuống.
    - Tìm các file code (theo phần mở rộng), bỏ qua các folder/file rác
      (venv, __pycache__, .git, node_modules, ...).
    - Ghi ra 1 file .txt gồm đường dẫn tương đối + nội dung code trong code block:

        - ./relative/path/to/file.py

        ```py
        <nội dung file>
        ```

UNPACK ("giải nén"):
    - Đọc file .txt (dạng trên) và tái tạo lại toàn bộ cây thư mục + file.

Usage:
    # Nén
    python3 pack_project.py pack
    python3 pack_project.py pack -r ./cmds -o cmds_dump.txt
    python3 pack_project.py pack -e py,yaml,sql
    python3 pack_project.py pack --include-all      # gộp mọi loại file text
    python3 pack_project.py pack -x old-architecture       # loại thêm folder

    # Giải nén
    python3 pack_project.py unpack project_dump.txt
    python3 pack_project.py unpack project_dump.txt -o ./restored
    python3 pack_project.py unpack project_dump.txt --dry-run
    python3 pack_project.py unpack project_dump.txt --overwrite
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

# Phần mở rộng được coi là "code" (mặc định).
DEFAULT_EXTENSIONS = {
    "py", "pyw", "pyi",
    "js", "jsx", "ts", "tsx",
    "java", "kt", "scala",
    "c", "h", "cpp", "hpp", "cc",
    "cs", "go", "rs", "rb", "php",
    "sh", "bash", "ps1", "bat", "cmd",
    "sql", "csv",
    "yaml", "yml", "json", "toml", "ini", "cfg", "conf",
    "html", "css", "scss",
    "md", "txt",
    "dockerfile",
    # File mẫu cấu hình: .env.example, settings.template, ...
    "example", "template", "sample",
}

# File không có extension (hoặc tên đặc biệt) vẫn cần lấy. So khớp theo TÊN
# ĐẦY ĐỦ, viết thường. Cần tập riêng vì `.gitignore` có suffix rỗng nhưng tên
# lại bắt đầu bằng dấu chấm nên không khớp được với danh sách extension.
DEFAULT_FILENAMES = {
    ".gitignore", ".gitattributes", ".dockerignore",
    ".editorconfig", ".npmrc", ".nvmrc", ".prettierrc",
    "makefile", "procfile", "dockerfile", "conf",
}

# File chứa secret thật — LUÔN bỏ qua, kể cả khi dùng --include-all.
# Chỉ cần lỡ pack một lần là client secret / token nằm trong file dump và rất
# dễ bị commit hoặc gửi đi kèm.
SECRET_FILENAMES = {
    ".env", ".env.local", ".env.development", ".env.production",
    "secrets.yaml", "secrets.yml", "credentials", ".netrc", ".pgpass",
    "id_rsa", "id_ed25519",
}

# Biến thể mẫu của .env thì vẫn lấy: chúng chỉ có khoá, không có giá trị thật.
SECRET_SAFE_SUFFIXES = (".example", ".template", ".sample")

# Map extension -> language tag cho code block (fallback = chính extension đó).
LANG_MAP = {
    "py": "py", "pyw": "py", "pyi": "py",
    "js": "javascript", "jsx": "jsx",
    "ts": "typescript", "tsx": "tsx",
    "yml": "yaml", "yaml": "yaml",
    "sh": "bash", "bash": "bash", "ps1": "powershell",
    "bat": "bat", "cmd": "bat",
    "md": "markdown",
    # .env.example và họ hàng đều là KEY=value.
    "example": "ini", "template": "ini", "sample": "ini", "env": "ini",
}

# Language tag cho file không có extension, tra theo tên đầy đủ.
FILENAME_LANG = {
    "dockerfile": "dockerfile",
    "conf": "nginx",
    "makefile": "makefile",
    ".gitignore": "gitignore",
    ".dockerignore": "gitignore",
}

# Các thư mục luôn bỏ qua.
DEFAULT_EXCLUDE_DIRS = {
    ".git", ".svn", ".hg",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    ".venv", "venv", "env", "ENV",
    "node_modules", ".next", ".nuxt", ".vite",
    "dist", "build", "out", ".idea", ".vscode",
    ".kiro", "data", "backup", ".venv-jupyter",
}


def resolve_lang(ext: str, name: str = "") -> str:
    """Language tag cho code block, ưu tiên extension rồi mới tới tên file."""
    if ext:
        return LANG_MAP.get(ext, ext)
    return FILENAME_LANG.get(name.lower(), "text")


def is_secret_file(name: str) -> bool:
    """File có khả năng chứa secret thật → không bao giờ đưa vào dump."""
    lower = name.lower()
    if lower.endswith(SECRET_SAFE_SUFFIXES):
        return False
    if lower in SECRET_FILENAMES:
        return True
    # Bắt các biến thể chưa liệt kê: .env.uat, .env.staging, ...
    return lower.startswith(".env")


def should_include(name: str, ext: str, extensions: set[str] | None) -> bool:
    """File này có được đưa vào dump không (đã bỏ qua khâu secret)."""
    lower = name.lower()
    if extensions is None:  # --include-all
        return True
    if lower in DEFAULT_FILENAMES:
        return True
    if ext:
        return ext in extensions
    # Không có extension: cho phép khai tên trong danh sách extension để giữ
    # tương thích với cách dùng `-e dockerfile,conf`.
    return lower in extensions


def read_text_safe(path: Path) -> str | None:
    """Đọc file text; trả None nếu là file binary hoặc không đọc được."""
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except (UnicodeDecodeError, ValueError):
            continue
        except OSError:
            return None
    return None


def iter_code_files(
    root: Path,
    extensions: set[str] | None,
    exclude_dirs: set[str],
    output_file: Path,
    script_file: Path,
) -> list[Path]:
    """Duyệt đệ quy, trả về danh sách file code (đã sort) tính từ root."""
    collected: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Cắt tỉa thư mục bị loại trực tiếp trên dirnames để không đi vào.
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for name in filenames:
            fpath = Path(dirpath) / name
            # Không tự đưa file output và chính script vào kết quả.
            if fpath.resolve() in (output_file.resolve(), script_file.resolve()):
                continue
            # Chặn secret trước mọi thứ khác, kể cả khi --include-all.
            if is_secret_file(name):
                continue
            ext = fpath.suffix.lstrip(".").lower()
            if not should_include(name, ext, extensions):
                continue
            collected.append(fpath)
    return sorted(collected)


def to_posix_rel(path: Path, root: Path) -> str:
    """Đường dẫn tương đối dạng ./a/b/c.py."""
    rel = path.relative_to(root).as_posix()
    return f"./{rel}"


def pack(
    root: Path,
    output_file: Path,
    extensions: set[str] | None,
    exclude_dirs: set[str],
    script_file: Path,
) -> tuple[int, int]:
    """Ghi tất cả file code vào output_file. Trả (số file ghi, số file bỏ qua)."""
    files = iter_code_files(root, extensions, exclude_dirs, output_file, script_file)

    written, skipped = 0, 0
    with output_file.open("w", encoding="utf-8") as out:
        for fpath in files:
            content = read_text_safe(fpath)
            if content is None:
                skipped += 1
                continue

            rel = to_posix_rel(fpath, root)
            lang = resolve_lang(fpath.suffix.lstrip(".").lower(), fpath.name)

            out.write(f"- {rel}\n\n")
            out.write(f"```{lang}\n")
            out.write(content)
            if not content.endswith("\n"):
                out.write("\n")
            out.write("```\n\n")
            written += 1

    return written, skipped


# --- UNPACK ---------------------------------------------------------------

# Header của mỗi file: "- ./path/to/file.ext"
HEADER_RE = re.compile(r"^- (\./.+?)\s*$")


def parse_entries(lines: list[str]) -> list[tuple[str, str]]:
    """
    Parse các block (path, content) từ nội dung dump.

    Chỉ coi 1 dòng là header khi nó khớp "- ./..." VÀ dòng non-blank kế tiếp
    là 1 fence mở (```...). Nhờ đó, các dòng dạng "- ./x" nằm bên trong nội
    dung markdown sẽ không bị nhận nhầm thành header.

    Với mỗi block, chỉ bóc fence MỞ đầu tiên và fence ĐÓNG cuối cùng của block
    -> nội dung có fence ``` lồng nhau (file .md) vẫn được giữ nguyên.
    """
    n = len(lines)
    headers: list[tuple[int, int, str]] = []  # (header_idx, open_fence_idx, path)

    for i, line in enumerate(lines):
        m = HEADER_RE.match(line)
        if not m:
            continue
        j = i + 1
        while j < n and lines[j].strip() == "":
            j += 1
        if j < n and lines[j].lstrip().startswith("```"):
            headers.append((i, j, m.group(1)))

    entries: list[tuple[str, str]] = []
    for idx, (_hi, fence_idx, path) in enumerate(headers):
        region_end = headers[idx + 1][0] if idx + 1 < len(headers) else n
        region = lines[fence_idx + 1:region_end]

        # Bỏ dòng trắng ở cuối, rồi bỏ fence đóng cuối cùng.
        while region and region[-1].strip() == "":
            region.pop()
        if region and region[-1].strip().startswith("```"):
            region.pop()

        content = "\n".join(region)
        entries.append((path, content))

    return entries


def normalize_rel_path(raw: str) -> str:
    """Chuẩn hóa path './a/b' -> 'a/b', chặn path tuyệt đối / traversal."""
    rel = raw.strip()
    if rel.startswith("./"):
        rel = rel[2:]
    rel = rel.replace("\\", "/")
    parts = [p for p in rel.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise ValueError(f"Path khong hop le (chua '..'): {raw}")
    if not parts:
        raise ValueError(f"Path rong: {raw}")
    return "/".join(parts)


def unpack(
    input_file: Path,
    out_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> tuple[int, int]:
    """Đọc dump file và tái tạo cây file. Trả (số file ghi, số file bỏ qua)."""
    text = read_text_safe(input_file)
    if text is None:
        raise SystemExit(f"Không đọc được file: {input_file}")

    entries = parse_entries(text.splitlines())
    if not entries:
        raise SystemExit("Không tìm thấy block file nào trong dump.")

    written, skipped = 0, 0
    for raw_path, content in entries:
        try:
            rel = normalize_rel_path(raw_path)
        except ValueError as e:
            print(f"  ! Bỏ qua: {e}")
            skipped += 1
            continue

        target = out_dir / rel

        if target.exists() and not overwrite:
            print(f"  = Đã tồn tại (bỏ qua): {rel}")
            skipped += 1
            continue

        if dry_run:
            print(f"  + {rel}")
            written += 1
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        # Ghi kèm newline cuối cho gọn (POSIX style).
        data = content if content.endswith("\n") else content + "\n"
        target.write_text(data, encoding="utf-8")
        print(f"  + {rel}")
        written += 1

    return written, skipped


# --- CLI ------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='"Nén"/"giải nén" source code của project qua 1 file .txt.'
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_pack = sub.add_parser("pack", help='"Nén" project vào 1 file .txt')
    p_pack.add_argument(
        "-r", "--root", default=".",
        help="Thư mục gốc để scan (mặc định: thư mục hiện tại).",
    )
    p_pack.add_argument(
        "-o", "--output", default="project_dump.txt",
        help="File .txt output (mặc định: project_dump.txt).",
    )
    p_pack.add_argument(
        "-e", "--extensions", default=None,
        help="Danh sách extension cần lấy, phân tách bằng dấu phẩy "
             "(vd: py,yaml,sql). Mặc định dùng danh sách code phổ biến.",
    )
    p_pack.add_argument(
        "-x", "--exclude", default=None,
        help="Thêm thư mục cần loại, phân tách bằng dấu phẩy (vd: jobs,docs).",
    )
    p_pack.add_argument(
        "--include-all", action="store_true",
        help="Bỏ lọc extension, gộp mọi file (file binary vẫn tự động bị bỏ).",
    )

    p_unpack = sub.add_parser("unpack", help='"Giải nén" file .txt thành project')
    p_unpack.add_argument(
        "input", help="File .txt dump cần giải nén.",
    )
    p_unpack.add_argument(
        "-o", "--out-dir", default=".",
        help="Thư mục đích để tái tạo project (mặc định: thư mục hiện tại).",
    )
    p_unpack.add_argument(
        "--overwrite", action="store_true",
        help="Ghi đè file đã tồn tại (mặc định: bỏ qua).",
    )
    p_unpack.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ liệt kê file sẽ tạo, không ghi ra đĩa.",
    )
    return parser


def run_pack(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    if not root.is_dir():
        raise SystemExit(f"Root không phải thư mục: {root}")

    output_file = Path(args.output)
    if not output_file.is_absolute():
        output_file = root / output_file

    if args.include_all:
        extensions = None
    elif args.extensions:
        extensions = {e.strip().lstrip(".").lower() for e in args.extensions.split(",") if e.strip()}
    else:
        extensions = set(DEFAULT_EXTENSIONS)

    exclude_dirs = set(DEFAULT_EXCLUDE_DIRS)
    if args.exclude:
        exclude_dirs.update(d.strip() for d in args.exclude.split(",") if d.strip())

    written, skipped = pack(root, output_file, extensions, exclude_dirs, Path(__file__))

    print(f"Root       : {root}")
    print(f"Output     : {output_file}")
    print(f"Đã ghi     : {written} file")
    if skipped:
        print(f"Bỏ qua     : {skipped} file (binary/không đọc được)")


def run_unpack(args: argparse.Namespace) -> None:
    input_file = Path(args.input).resolve()
    if not input_file.is_file():
        raise SystemExit(f"Không tìm thấy file dump: {input_file}")

    out_dir = Path(args.out_dir).resolve()

    written, skipped = unpack(input_file, out_dir, args.overwrite, args.dry_run)

    print("-" * 50)
    print(f"Input      : {input_file}")
    print(f"Out dir    : {out_dir}")
    prefix = "Sẽ tạo    " if args.dry_run else "Đã ghi    "
    print(f"{prefix} : {written} file")
    if skipped:
        print(f"Bỏ qua     : {skipped} file")


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "pack":
        run_pack(args)
    elif args.command == "unpack":
        run_unpack(args)


if __name__ == "__main__":
    main()
