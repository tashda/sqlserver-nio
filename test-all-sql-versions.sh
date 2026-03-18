#!/bin/bash
set -euo pipefail

# Runs the package test suite against each supported SQL Server version.
# SQL Server 2008 R2-2016 are executed on the 2017 Linux engine with the
# matching database compatibility level, because Microsoft does not publish
# Linux container images for those releases.

VERSIONS=("2008R2" "2012" "2014" "2016" "2017-latest" "2019-latest" "2022-latest" "2025-latest")
EXIT_CODE=0

for VERSION in "${VERSIONS[@]}"; do
  echo
  echo "============================================================"
  echo "Testing SQL Server version: ${VERSION}"
  echo "============================================================"

  export USE_DOCKER=1
  export TDS_DOCKER_PORT=14331
  export TDS_VERSION="${VERSION}"
  export TDS_LOAD_ADVENTUREWORKS=1
  export TDS_AW_DATABASE=AdventureWorks

  if ! swift test; then
    echo "Tests failed for ${VERSION}"
    EXIT_CODE=1
  fi

done

exit ${EXIT_CODE}
