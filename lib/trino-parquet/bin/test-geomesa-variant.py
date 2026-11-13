#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate real GeoMesa writer fixtures and run Trino's explicit reader gate.

Bootstrap each repository's module dependencies first; this runner never changes
toolchains, repositories, build profiles or server defaults. See docs/variant/BUILD.md.
"""

import argparse
from contextlib import nullcontext
from pathlib import Path
import shlex
import subprocess
import tempfile
import xml.etree.ElementTree as ET


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geomesa-checkout", required=True, type=Path)
    parser.add_argument("--geomesa-maven", default="mvn", help="Maven executable on PATH or an absolute path")
    parser.add_argument("--trino-maven", help="Override Trino's checked-in Maven wrapper executable")
    parser.add_argument("--geomesa-maven-repo", type=Path, help="Honor this Maven local repository for fixture generation")
    parser.add_argument("--trino-maven-repo", type=Path, help="Fork-test Maven repository, distinct from stock-plugin packaging")
    parser.add_argument("--geomesa-toolchains", type=Path, help="Use the supplied Maven toolchains file")
    parser.add_argument("--fixture-directory", type=Path, help="Preserve fixtures here; existing fixture files are never overwritten")
    parser.add_argument("--offline", action="store_true", help="Use only already cached dependencies")
    parser.add_argument("--dry-run", action="store_true", help="Print the two commands without invoking Maven")
    return parser.parse_args()


def run(command, checkout, dry_run):
    print(f"{checkout}: {shlex.join(command)}", flush=True)
    if not dry_run:
        subprocess.run(command, cwd=checkout, check=True)


def main():
    args = arguments()
    trino = Path(__file__).resolve().parents[3]
    geomesa = args.geomesa_checkout.resolve()
    geomesa_pom = geomesa / "geomesa-fs/geomesa-fs-storage/pom.xml"
    if not geomesa_pom.is_file():
        raise SystemExit(f"GeoMesa storage module not found: {geomesa_pom}")

    output = (
        nullcontext(str(args.fixture_directory.resolve()))
        if args.fixture_directory
        else tempfile.TemporaryDirectory(prefix="geomesa-trino-variant-")
    )
    with output as directory:
        fixtures = Path(directory)
        names = ("expected.json", "plain.parquet", "object.parquet", "array.parquet")
        if any((fixtures / name).exists() for name in names):
            raise SystemExit("Fixture output already exists; choose a directory without fixture files")

        writer = [args.geomesa_maven, "-B"]
        reader = [args.trino_maven or str(trino / "mvnw"), "-B"]
        if args.offline:
            writer.append("-o")
            reader.append("-o")
        for command, repository in ((writer, args.geomesa_maven_repo), (reader, args.trino_maven_repo)):
            if repository is not None:
                command.append(f"-Dmaven.repo.local={repository.resolve()}")
        if args.geomesa_toolchains:
            writer.extend(["-t", str(args.geomesa_toolchains.resolve())])
        fixture_option = f"-Dgeomesa.variant.fixture-directory={fixtures}"
        writer.extend(["-f", str(geomesa_pom), "test", "-Dtest=GeoMesaVariantFixtureTest", fixture_option])
        reader.extend(["-pl", "lib/trino-parquet", "test", "-Dtest=GeoMesaVariantCompatibilityIT", fixture_option])

        report = trino / "lib/trino-parquet/target/surefire-reports/TEST-io.trino.parquet.reader.GeoMesaVariantCompatibilityIT.xml"
        previous_report = report.stat().st_mtime_ns if report.exists() else None
        run(writer, geomesa, args.dry_run)
        if not args.dry_run and not all((fixtures / name).is_file() for name in names):
            raise SystemExit("GeoMesa did not produce the complete validated fixture set")
        run(reader, trino, args.dry_run)
        if not args.dry_run:
            if not report.exists() or report.stat().st_mtime_ns == previous_report:
                raise SystemExit("Trino produced no fresh compatibility test report")
            result = ET.parse(report).getroot()
            if int(result.get("tests", "0")) != 4 or any(int(result.get(key, "0")) for key in ("failures", "errors", "skipped")):
                raise SystemExit(f"Compatibility gate did not pass all four checks: {result.attrib}")
            print("GeoMesa writer → Trino reader: all four compatibility checks passed", flush=True)


if __name__ == "__main__":
    main()
