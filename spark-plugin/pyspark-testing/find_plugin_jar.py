"""
Locate the DataFlint plugin JAR for the current Spark version.

Usage:
    from find_plugin_jar import find_plugin_jar
    plugin_jar = find_plugin_jar()
"""
import os
import re
from pathlib import Path


def find_plugin_jar() -> Path:
    """Find the DataFlint plugin JAR, supporting both release and SNAPSHOT builds."""
    project_root = Path(__file__).resolve().parent.parent

    # Detect Spark major version from SPARK_HOME
    spark_home = os.environ.get("SPARK_HOME", "")
    spark_major_version = 3  # default

    if spark_home:
        m = re.search(r"[/_-](\d+)\.\d", spark_home)
        if m:
            spark_major_version = int(m.group(1))

    # Select module and artifact pattern based on Spark version
    if spark_major_version == 4:
        module = "pluginspark4"
        scala_dir = "scala-2.13"
        artifact_prefix = "dataflint-spark4_2.13-"
    else:
        module = "pluginspark3"
        scala_dir = "scala-2.12"
        artifact_prefix = "spark_2.12-"

    jar_dir = project_root / module / "target" / scala_dir

    # Look for JARs: prefer release, fall back to SNAPSHOT
    if jar_dir.exists():
        jars = sorted(jar_dir.glob(f"{artifact_prefix}*.jar"), reverse=True)
        # Filter out sources/javadoc JARs
        jars = [j for j in jars if "-sources" not in j.name and "-javadoc" not in j.name]
        # Prefer non-SNAPSHOT
        release_jars = [j for j in jars if "SNAPSHOT" not in j.name]
        if release_jars:
            return release_jars[0]
        if jars:
            return jars[0]

    raise FileNotFoundError(
        f"Plugin JAR not found in {jar_dir}\n"
        f"Run: cd {project_root} && sbt {module}/assembly"
    )