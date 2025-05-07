#!/bin/bash -e
# prevent partial execution if download is partial for some reason
main () {
    OS=$(uname -s)
    ARCH=$(uname -m)


    command -v curl >/dev/null 2>&1 || { echo >&2 "Required tool curl could not be found. Aborting."; exit 1; }
    command -v zcat >/dev/null 2>&1 || { echo >&2 "Required tool zcat could not be found. Hint: install the gzip package. Aborting."; exit 1; }

    # figure out latest version
    VER=1.2.2
    eval PREFIX="~/.duckdb/cli"
    INST="${PREFIX}/${VER}"
    LATEST="${PREFIX}/latest"

    rm -rf $INST
    rm -rf $LATEST

    DIST=

    if [ "${OS}" = "Linux" ]
    then
        if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]
        then
            DIST=linux-amd64
        elif [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]
        then
            DIST=linux-aarch64
        fi
    elif [ "${OS}" = "Darwin" ]
    then
        DIST=osx-universal
    fi

    if [ -z "${DIST}" ]
    then
        echo "Operating system '${OS}' / architecture '${ARCH}' is unsupported." 1>&2
        exit 1
    fi

    URL="https://github.com/duckdb/duckdb/releases/download/v${VER}/duckdb_cli-${DIST}.gz"
    echo
    echo "*** DuckDB Linux/MacOS installation script, version ${VER} ***"
    echo
    echo
    echo "         .;odxdl,            "
    echo "       .xXXXXXXXXKc          "
    echo "       0XXXXXXXXXXXd  cooo:  "
    echo "      ,XXXXXXXXXXXXK  OXXXXd "
    echo "       0XXXXXXXXXXXo  cooo:  "
    echo "       .xXXXXXXXXKc          "
    echo "         .;odxdl,  "
    echo
    echo

    if [ -f "${INST}/duckdb" ]; then
        echo "Destination binary ${INST}/duckdb already exists"
    else
        mkdir -p "${INST}"

        if [ ! -d "${INST}" ]; then
            echo "Failed to create install directory ${INST}." 1>&2
            exit 1
        fi

        curl --fail --location --progress-bar "${URL}" | zcat > "${INST}/duckdb" && chmod a+x "${INST}/duckdb" || exit 1


        if [ ! -f "${INST}/duckdb" ]; then
            echo "Failed to download/unpack binary at ${INST}/duckdb" 1>&2
            exit 1
        fi

        # lets test if this works
        TEST=$("${INST}/duckdb" -noheader -init /dev/null -csv -batch -s "SELECT 2*3*7")

        if  [ ! "$TEST" = "42" ]; then
            echo "Failed to execute installed binary :/ ${INST}." 1>&2
            exit 1
        fi
        echo
        echo "Successfully installed DuckDB binary to ${INST}/duckdb"
        rm -f "${LATEST}" || exit 1
        ln -s "${INST}" "${LATEST}" || exit 1
        echo "  with a link from                      ${LATEST}/duckdb"
    fi

    echo
    echo "Hint: Append the following line to your shell profile:"
    echo "export PATH='${LATEST}':\$PATH"
    echo

    # maybe ~/.local/bin exists and is writeable and does not have duckdb yet
    # if so, ask user if they would like a symlink
    eval LOCALBIN="${HOME}/.local/bin"
    if [ -d "${LOCALBIN}" ] && [ -w "${LOCALBIN}" ] && [ ! -f "${LOCALBIN}/duckdb" ]; then
        ln -s "${LATEST}/duckdb" "${LOCALBIN}/duckdb" || exit 1
        echo "Also created a symlink from ${LOCALBIN}/duckdb
                         to ${LATEST}/duckdb"
    fi

    echo
    echo "To launch DuckDB now, type"
    echo "${LATEST}/duckdb"
}
main