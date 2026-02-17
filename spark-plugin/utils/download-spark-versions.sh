#!/usr/bin/env bash
#
# Downloads Apache Spark distributions for all supported versions (3.0.x – 4.1.x).
# Each version is extracted into .spark-versions/<version>/ under this directory.
#
# Usage:
#   ./download-spark-versions.sh          # download all versions
#   ./download-spark-versions.sh 3.5.8    # download a single version
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOWNLOAD_DIR="${SCRIPT_DIR}/.spark-versions"
mkdir -p "${DOWNLOAD_DIR}"

ARCHIVE_BASE="https://archive.apache.org/dist/spark"

# version:hadoop_suffix pairs (portable — no associative arrays needed)
ALL_VERSIONS="
3.0.3:hadoop3.2
3.1.3:hadoop3.2
3.2.4:hadoop3.2
3.3.4:hadoop3
3.4.4:hadoop3
3.5.8:hadoop3
4.0.2:hadoop3
4.1.0:hadoop3
"

get_hadoop_suffix() {
  local ver="$1"
  echo "${ALL_VERSIONS}" | grep "^${ver}:" | cut -d: -f2
}

known_version() {
  echo "${ALL_VERSIONS}" | grep -q "^${1}:"
}

# Older PySpark versions (< 3.4) bundle a cloudpickle that doesn't support Python 3.12+.
# This function replaces the bundled cloudpickle in both:
#   - python/pyspark/cloudpickle/  (used by the driver)
#   - python/lib/pyspark.zip       (used by workers)
patch_cloudpickle() {
  local dest_dir="$1"
  local version="$2"
  local pyspark_cp="${dest_dir}/python/pyspark/cloudpickle"
  local pyspark_zip="${dest_dir}/python/lib/pyspark.zip"

  # Only patch Spark < 3.4 (those bundle cloudpickle < 2.2 which breaks on Python 3.12+)
  local minor
  minor="$(echo "${version}" | cut -d. -f1-2)"
  case "${minor}" in
    3.0|3.1|3.2|3.3) ;;
    *) return 0 ;;
  esac

  if [ -f "${pyspark_cp}/.patched" ]; then
    return 0
  fi

  echo "🔧 Patching cloudpickle for Spark ${version} (Python 3.12+ compatibility) ..."
  local tmp_dir="${DOWNLOAD_DIR}/.cloudpickle_tmp"
  rm -rf "${tmp_dir}"
  mkdir -p "${tmp_dir}"

  if ! python3 -m pip install --quiet --target="${tmp_dir}" 'cloudpickle>=3.0'; then
    echo "  ⚠ Could not install cloudpickle — pip install failed. Old Spark versions may not work with Python 3.12+"
    rm -rf "${tmp_dir}"
    return 1
  fi

  # 1) Patch the driver-side cloudpickle
  rm -rf "${pyspark_cp}"
  cp -r "${tmp_dir}/cloudpickle" "${pyspark_cp}"

  # 2) Patch cloudpickle inside pyspark.zip (used by workers)
  if [ -f "${pyspark_zip}" ]; then
    local zip_work="${DOWNLOAD_DIR}/.pyspark_zip_work"
    rm -rf "${zip_work}"
    mkdir -p "${zip_work}"
    unzip -q "${pyspark_zip}" -d "${zip_work}"
    rm -rf "${zip_work}/pyspark/cloudpickle"
    cp -r "${tmp_dir}/cloudpickle" "${zip_work}/pyspark/cloudpickle"
    rm -f "${pyspark_zip}"
    ( cd "${zip_work}" && zip -qr "${pyspark_zip}" . )
    rm -rf "${zip_work}"
    echo "  ✓ cloudpickle patched (driver + worker pyspark.zip)"
  else
    echo "  ✓ cloudpickle patched (driver only — pyspark.zip not found)"
  fi

  touch "${pyspark_cp}/.patched"
  rm -rf "${tmp_dir}"
}

download_spark() {
  local version="$1"
  local hadoop_suffix="$2"
  local dest_dir="${DOWNLOAD_DIR}/${version}"

  if [ -d "${dest_dir}" ] && [ -f "${dest_dir}/bin/spark-submit" ]; then
    echo "✓ Spark ${version} already downloaded at ${dest_dir}"
    patch_cloudpickle "${dest_dir}" "${version}"
    return 0
  fi

  local filename="spark-${version}-bin-${hadoop_suffix}.tgz"
  local url="${ARCHIVE_BASE}/spark-${version}/${filename}"
  local tmp_file="${DOWNLOAD_DIR}/${filename}"

  echo "⬇ Downloading Spark ${version} from ${url} ..."
  if ! curl -fSL --progress-bar -o "${tmp_file}" "${url}"; then
    echo "✗ Failed to download Spark ${version} — skipping"
    rm -f "${tmp_file}"
    return 1
  fi

  echo "📦 Extracting Spark ${version} ..."
  mkdir -p "${dest_dir}"
  tar -xzf "${tmp_file}" -C "${dest_dir}" --strip-components=1
  rm -f "${tmp_file}"

  patch_cloudpickle "${dest_dir}" "${version}"

  echo "✓ Spark ${version} installed at ${dest_dir}"
}

if [ $# -ge 1 ]; then
  requested="$1"
  hadoop_suffix="$(get_hadoop_suffix "${requested}")"
  if [ -z "${hadoop_suffix}" ]; then
    echo "Unknown version: ${requested}"
    echo "Available versions:"
    echo "${ALL_VERSIONS}" | grep ':' | cut -d: -f1 | sed 's/^/  /'
    exit 1
  fi
  download_spark "${requested}" "${hadoop_suffix}"
else
  echo "Downloading all Spark versions to ${DOWNLOAD_DIR}"
  echo ""
  echo "${ALL_VERSIONS}" | grep ':' | sort -t. -k1,1n -k2,2n -k3,3n | while IFS=: read -r version hadoop_suffix; do
    download_spark "${version}" "${hadoop_suffix}" || true
    echo ""
  done
  echo "Done! All available versions are in ${DOWNLOAD_DIR}/"
fi
