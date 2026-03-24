import argparse
import fnmatch
import os


def load_gitignore_patterns(folder):
    patterns = []
    gitignore_path = os.path.join(folder, ".gitignore")
    if os.path.exists(gitignore_path):
        with open(gitignore_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    patterns.append(line.rstrip("/"))
    return patterns


def is_ignored(name, rel_path, patterns):
    for pattern in patterns:
        if fnmatch.fnmatch(name, pattern):
            return True
        if fnmatch.fnmatch(rel_path, pattern):
            return True
        # match patterns like *.pyc or __pycache__ anywhere in the tree
        if fnmatch.fnmatch(os.path.basename(rel_path), pattern):
            return True
    return False


def print_tree(path, root, inherited_patterns, prefix="", extra_exclude=set()):
    # Load .gitignore in current folder and merge with inherited
    local_patterns = load_gitignore_patterns(path)
    patterns = inherited_patterns + local_patterns

    try:
        entries = sorted(os.listdir(path))
    except PermissionError:
        return

    entries = [
        e
        for e in entries
        if e not in extra_exclude
        and e != ".git"
        and not is_ignored(e, os.path.relpath(os.path.join(path, e), root), patterns)
    ]

    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        print(prefix + connector + entry)

        full_path = os.path.join(path, entry)
        if os.path.isdir(full_path):
            extension = "    " if i == len(entries) - 1 else "│   "
            print_tree(full_path, root, patterns, prefix + extension, extra_exclude)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Print directory tree respecting all .gitignore files"
    )
    parser.add_argument("path", nargs="?", default=".", help="Root path")
    parser.add_argument(
        "--exclude", nargs="*", default=[], help="Extra files/folders to exclude"
    )
    args = parser.parse_args()

    root = os.path.abspath(args.path)
    root_patterns = load_gitignore_patterns(root)

    print(root)
    print_tree(root, root, root_patterns, extra_exclude=set(args.exclude))


# Respeta el .gitignore del proyecto
# python tree.py

# Agregar exclusiones extra además del .gitignore
# python tree.py --exclude data models
