#!/usr/bin/env python3
"""Build and run pub-sub GoogleTest suites."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SUITES = (
    "message_broker_tests",
    "compete_tests",
    "fanout_tests",
    "subscriber_lifecycle_tests",
)

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_BUILD_DIR = PROJECT_ROOT / "build"


def executable_path(build_dir: Path, suite: str) -> Path:
    name = f"{suite}.exe" if sys.platform == "win32" else suite
    return build_dir / name


def run_command(cmd: list[str], *, cwd: Path | None = None) -> int:
    print(f"\n>> {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd).returncode


def cmake_build(build_dir: Path) -> int:
    build_dir.mkdir(parents=True, exist_ok=True)
    rc = run_command(["cmake", "-S", str(PROJECT_ROOT), "-B", str(build_dir)])
    if rc != 0:
        return rc
    return run_command(["cmake", "--build", str(build_dir)])


def run_ctest(build_dir: Path, *, verbose: bool) -> int:
    cmd = ["ctest", "--test-dir", str(build_dir), "--output-on-failure"]
    if verbose:
        cmd.append("-V")
    return run_command(cmd)


def run_suite(build_dir: Path, suite: str, gtest_args: list[str]) -> int:
    exe = executable_path(build_dir, suite)
    if not exe.is_file():
        print(f"error: {exe} not found — run with --build first", file=sys.stderr)
        return 1
    return run_command([str(exe), *gtest_args])


def run_suites(build_dir: Path, suites: list[str], gtest_args: list[str]) -> int:
    failed = False
    for suite in suites:
        if run_suite(build_dir, suite, gtest_args) != 0:
            failed = True
    return 1 if failed else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pub-sub GoogleTest suites.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""examples:
  %(prog)s --build                 Configure, build, and run all tests (ctest)
  %(prog)s                         Run all tests from an existing build
  %(prog)s -s fanout_tests         Run one suite
  %(prog)s -s compete_tests -s fanout_tests
  %(prog)s -s fanout_tests -- --gtest_filter='FanoutRouting.*'
""",
    )
    parser.add_argument(
        "--build-dir",
        type=Path,
        default=DEFAULT_BUILD_DIR,
        help=f"CMake build directory (default: {DEFAULT_BUILD_DIR.name}/)",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Configure and build with CMake before running tests",
    )
    parser.add_argument(
        "-s",
        "--suite",
        action="append",
        dest="suites",
        choices=SUITES,
        metavar="SUITE",
        help="Run only the given suite (repeatable)",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List available suites and exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output when running all suites via ctest",
    )
    parser.add_argument(
        "--executable",
        action="store_true",
        help="Run suite binaries directly instead of ctest (used automatically with --suite)",
    )
    parser.add_argument(
        "gtest_args",
        nargs=argparse.REMAINDER,
        help="Extra gtest args after -- (e.g. --gtest_filter=Suite.*)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.list:
        for suite in SUITES:
            print(suite)
        return 0

    build_dir = args.build_dir.resolve()
    gtest_args = args.gtest_args
    if gtest_args and gtest_args[0] == "--":
        gtest_args = gtest_args[1:]

    if args.build:
        rc = cmake_build(build_dir)
        if rc != 0:
            return rc

    if args.suites:
        return run_suites(build_dir, args.suites, gtest_args)

    if args.executable:
        return run_suites(build_dir, list(SUITES), gtest_args)

    ctest_file = build_dir / "CTestTestfile.cmake"
    if not ctest_file.is_file():
        print(
            f"error: {build_dir} is not a configured CMake build directory.\n"
            "Run with --build or pass --executable to run suite binaries directly.",
            file=sys.stderr,
        )
        return 1

    return run_ctest(build_dir, verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
