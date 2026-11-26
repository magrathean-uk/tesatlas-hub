#!/bin/zsh
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail
# Earlier variable names still work for one release (DEC-160).
[ -n "${TESATLAS_LAB-}" ] || [ -z "${TESLATLAS_LAB-}" ] || export TESATLAS_LAB="$TESLATLAS_LAB" # rename:keep

postgres_bin=/opt/homebrew/opt/postgresql@18/bin
schema_dump=${TESATLAS_TEST_SOURCE_SCHEMA_DUMP:?set TESATLAS_TEST_SOURCE_SCHEMA_DUMP to the private source custom dump}
lab_tmp_root=${TESATLAS_TEST_PRIVATE_TMP_ROOT:-${TESATLAS_LAB:?set TESATLAS_LAB or TESATLAS_TEST_PRIVATE_TMP_ROOT}/tmp}
hub_root=${0:A:h:h}
workspace_root=${hub_root:h}

[[ -f ${schema_dump} ]] || {
  print -u2 'physical V3 test schema dump is not a regular file'
  exit 2
}
[[ -x ${postgres_bin}/initdb && -x ${postgres_bin}/pg_restore ]] || {
  print -u2 'PostgreSQL 18 test tools are unavailable'
  exit 2
}
[[ -d ${lab_tmp_root} ]] || {
  print -u2 'private lab temporary root is unavailable'
  exit 2
}

umask 077
fixture_root=$(mktemp -d "${lab_tmp_root%/}/physical-v3-pg.XXXXXX")
data_root=${fixture_root}/data
socket_root=${fixture_root}/socket

cleanup() {
  if [[ -s ${data_root}/postmaster.pid ]]; then
    "${postgres_bin}/pg_ctl" -D "${data_root}" -m immediate -w stop >/dev/null 2>&1 || true
  fi
  case ${fixture_root} in
    ${lab_tmp_root%/}/physical-v3-pg.*) rm -rf -- "${fixture_root}" ;;
    *) print -u2 'refusing to remove unexpected physical V3 fixture path' ;;
  esac
}
trap cleanup EXIT INT TERM

chmod 0700 "${fixture_root}"
mkdir -m 0700 "${socket_root}"
# A Unix socket path must fit sun_path (104 bytes on macOS) with PostgreSQL's ".s.PGSQL.<port>".
(( ${#socket_root} <= 85 )) || {
  print -u2 'private lab temporary root is too long for a PostgreSQL socket path'
  exit 2
}
fixture_port=$(python3 -c 'import socket; value = socket.socket(); value.bind(("127.0.0.1", 0)); print(value.getsockname()[1]); value.close()')

"${postgres_bin}/initdb" \
  -D "${data_root}" \
  --username=fixture_owner \
  --auth-local=trust \
  --auth-host=reject \
  --no-instructions >/dev/null
# Privileged access is reachable only through the Unix socket in the owner-only fixture
# directory, so only the OS principal running this script can use it. The loopback TCP
# listener (the reader under test connects by IP host) admits one unprivileged role with a
# password; any other role or database has no matching rule and is refused.
cat > "${data_root}/pg_hba.conf" <<'HBA'
local all fixture_owner trust
host source_fixture fixture_reader 127.0.0.1/32 scram-sha-256
HBA
reader_password=fixture-password
"${postgres_bin}/pg_ctl" \
  -D "${data_root}" \
  -l "${fixture_root}/postgres.log" \
  -o "-h 127.0.0.1 -p ${fixture_port} -k ${socket_root} -c unix_socket_permissions=0700" \
  -w start >/dev/null
"${postgres_bin}/createdb" \
  -h "${socket_root}" -p "${fixture_port}" -U fixture_owner source_fixture
"${postgres_bin}/pg_restore" \
  -h "${socket_root}" -p "${fixture_port}" -U fixture_owner \
  -d source_fixture \
  --schema-only --schema=public --no-owner --no-privileges \
  "${schema_dump}" >/dev/null
"${postgres_bin}/psql" \
  -h "${socket_root}" -p "${fixture_port}" -U fixture_owner \
  -d source_fixture -v ON_ERROR_STOP=1 -q <<SQL
CREATE ROLE fixture_reader LOGIN PASSWORD '${reader_password}';
GRANT CONNECT ON DATABASE source_fixture TO fixture_reader;
GRANT USAGE ON SCHEMA public TO fixture_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO fixture_reader;
SQL

# H14: the fixture must not give privileged database access to anything but the OS principal.
check_fixture_access() {
  local psql="${postgres_bin}/psql" fail=0 socket_file
  socket_file=${socket_root}/.s.PGSQL.${fixture_port}
  [[ $(stat -f %Lp "${socket_root}") == 700 && $(stat -f %Lp "${socket_file}") == 700 ]] || {
    print -u2 'fixture socket or its directory is not owner-only'; fail=1; }
  "${psql}" -h "${socket_root}" -p "${fixture_port}" -U fixture_owner -d source_fixture -w -Atqc 'SELECT 1' >/dev/null 2>&1 || {
    print -u2 'fixture owner cannot connect through the owner-only socket'; fail=1; }
  if "${psql}" -h 127.0.0.1 -p "${fixture_port}" -U fixture_owner -d source_fixture -w -Atqc 'SELECT 1' >/dev/null 2>&1; then
    print -u2 'fixture owner can connect over TCP'; fail=1
  fi
  if "${psql}" -h 127.0.0.1 -p "${fixture_port}" -U fixture_reader -d source_fixture -w -Atqc 'SELECT 1' >/dev/null 2>&1; then
    print -u2 'fixture reader can connect over TCP without its password'; fail=1
  fi
  if PGPASSWORD=wrong-password "${psql}" -h 127.0.0.1 -p "${fixture_port}" -U fixture_reader -d source_fixture -w -Atqc 'SELECT 1' >/dev/null 2>&1; then
    print -u2 'fixture reader can connect over TCP with a wrong password'; fail=1
  fi
  [[ $(PGPASSWORD=${reader_password} "${psql}" -h 127.0.0.1 -p "${fixture_port}" -U fixture_reader -d source_fixture -w -Atqc 'SELECT rolsuper FROM pg_roles WHERE rolname = current_user' 2>/dev/null) == f ]] || {
    print -u2 'fixture reader cannot connect over TCP with its password, or is a superuser'; fail=1; }
  return ${fail}
}
check_fixture_access || { print -u2 'physical V3 fixture access check failed'; exit 1; }
# Test seam for the access check alone: it needs PostgreSQL but not cargo.
[[ -z ${TESATLAS_FIXTURE_ACCESS_CHECK_ONLY-} ]] || exit 0

export TESATLAS_HUB_PHYSICAL_CAPTURE_TEST_POSTGRES_URL="postgresql://fixture_reader@127.0.0.1:${fixture_port}/source_fixture"
# The reader's password is the synthetic one the test supplies. The admin connection uses the socket.
export TESATLAS_HUB_PHYSICAL_CAPTURE_TEST_POSTGRES_ADMIN_URL="postgresql://fixture_owner@/source_fixture?host=${socket_root}&port=${fixture_port}"
# The test is #[ignore]d, so a cargo exit of zero is not enough: a filter that
# matches nothing would also exit zero. Require exactly one test run and passed.
test_log=${fixture_root}/cargo-test.log
"${workspace_root}/scripts/dev/with-heavy-build-lock.sh" \
  "${workspace_root}/scripts/dev/run.sh" hub cargo test --locked --lib \
    import::source_db::reader::tests::physical_v3_capture_uses_one_exported_snapshot_and_discards_hook_failures_when_configured \
    -- --ignored --exact --nocapture 2>&1 | tee "${test_log}"
if ! grep -qxF 'running 1 test' "${test_log}" ||
  ! grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "${test_log}"; then
  print -u2 'physical V3 capture test did not run exactly once and pass'
  exit 1
fi
