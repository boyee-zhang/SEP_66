#!/usr/bin/env bash

function cleanup {
    if [[ -n ${CONTAINER_ID:-} ]]; then
        docker rm -f "${CONTAINER_ID}"
    fi
}

function test_trino_starts {
    local QUERY_PERIOD=10
    local QUERY_RETRIES=90

    CONTAINER_ID=
    trap cleanup EXIT

    # We aren't passing --rm here to make sure container is available for inspection in case of failures
    CONTAINER_ID=$(docker run -d "$@")

    set +e
    I=0
    until docker inspect "${CONTAINER_ID}" --format "{{json .State.Health.Status }}" | grep -q '"healthy"'; do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "🚨 Too many retries waiting for Trino to start"
            echo "Logs from ${CONTAINER_ID} follow..."
            docker logs "${CONTAINER_ID}"
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    if ! RESULT=$(docker exec "${CONTAINER_ID}" trino --execute "SELECT 'success'" 2>/dev/null); then
        echo "🚨 Failed to execute a query after Trino container started"
    fi
    set -e

    cleanup
    trap - EXIT

    # Return proper exit code.
    [[ ${RESULT} == '"success"' ]]
}

function test_trino_fails {
    if ! timeout 10 docker run --rm "$@"; then
        if [ $? == 124 ]; then
            echo >&2 "Command expected to fail but did not: docker run --rm $*"
            exit 1
        fi
    fi
}

function test_javahome {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    # Check if JAVA_HOME works
    docker run --rm --platform "${PLATFORM}" "${CONTAINER_NAME}" \
        /bin/bash -c '$JAVA_HOME/bin/java -version' &>/dev/null

    [[ $? == "0" ]]
}

function test_container {
    local CONTAINER_NAME=$1
    local PLATFORM=$2
    echo "🐢 Validating ${CONTAINER_NAME} on platform ${PLATFORM}..."
    test_javahome "${CONTAINER_NAME}" "${PLATFORM}"
    test_trino_starts -e TRINO_ENABLE_PLUGINS=jmx,memory --platform "${PLATFORM}" "${CONTAINER_NAME}"
    test_trino_starts -e TRINO_DISABLE_PLUGINS=tpch,tpcds --platform "${PLATFORM}" "${CONTAINER_NAME}"
    test_trino_fails -e TRINO_ENABLE_PLUGINS=jmx,memory -e TRINO_DISABLE_PLUGINS=tpch,tpcds "${CONTAINER_NAME}"
    echo "🎉 Validated ${CONTAINER_NAME} on platform ${PLATFORM}"
}
