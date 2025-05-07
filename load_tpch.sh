#!/bin/bash
set -euo pipefail

# Logging function with improved timestamp and log levels
log() {
    local level="${2:-INFO}"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [$level] $1" >&2
}

# Error handling function with optional exit code
error_exit() {
    log "$1" "ERROR"
    exit "${2:-1}"
}

# Check and install required commands
ensure_command() {
    local cmd="$1"
    local install_cmd="${2:-}"

    if ! command -v "$cmd" >/dev/null 2>&1; then
        log "$cmd not found, attempting to install..." "WARN"
        if [[ -n "$install_cmd" ]]; then
            eval "$install_cmd" || error_exit "Failed to install $cmd"
        else
            error_exit "Command $cmd not found and no installation method provided"
        fi
    fi
}

# Validate and set configuration
configure_environment() {
    # Configurable parameters with sensible defaults
    SCALE_FACTOR="${TPCH_SCALE_FACTOR:-1}"
    DATA_DIR="${TPCH_DATA_DIR:-/home/iceberg/data/tpch_${SCALE_FACTOR}}"
    ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE_PATH:-s3://warehouse}"
    SPARK_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"

    log "Configuration: Scale Factor=$SCALE_FACTOR, Data Dir=$DATA_DIR"

    # Ensure data directory exists
    mkdir -p "$DATA_DIR" || error_exit "Failed to create data directory"
}

# Generate TPCH data using DuckDB
generate_tpch_data() {
    ensure_command "duckdb" "bash /home/iceberg/install_duckdb.sh && export PATH='/root/.duckdb/cli/latest':$PATH"

    log "Generating TPCH data with DuckDB (Scale Factor: $SCALE_FACTOR)"
    duckdb << EOF || error_exit "DuckDB data generation failed"
install tpch;
load tpch;
CALL dbgen(sf = $SCALE_FACTOR);
EXPORT DATABASE '$DATA_DIR' (FORMAT PARQUET);
EOF

    # Verify data generation
    local parquet_count
    parquet_count=$(find "$DATA_DIR" -name "*.parquet" | wc -l)
    [[ "$parquet_count" -eq 0 ]] && error_exit "No Parquet files generated"
    log "Generated $parquet_count Parquet files"
}

# Load data into Iceberg catalog using Spark
load_to_iceberg() {
    ensure_command "spark-shell"

    log "Loading data into Iceberg catalog"
    spark-shell --driver-memory "$SPARK_MEMORY" << EOF
import org.apache.spark.sql.SaveMode
import java.io.File

val icebergWarehouse = "$ICEBERG_WAREHOUSE"
val parquetDir = "$DATA_DIR"

val files = new File(parquetDir).listFiles.filter(_.getName.endsWith(".parquet"))

files.foreach { file =>
  val tableName = file.getName.stripSuffix(".parquet")
  val df = spark.read.parquet(file.getAbsolutePath)

  val fullyQualifiedTableName = s"demo.tpch.\$tableName"

  df.createOrReplaceTempView(tableName)

  spark.sql(s"""
    CREATE TABLE IF NOT EXISTS \$fullyQualifiedTableName
    USING iceberg
    LOCATION '\$icebergWarehouse/tpch/\$tableName'
    AS SELECT * FROM \$tableName
  """)

  println(s"Created table \$fullyQualifiedTableName from \${file.getName}")
}

spark.sql("SHOW TABLES IN demo.tpch").show()
System.exit(0)
EOF
}

# Main script execution
main() {
    log "Starting TPCH data generation and loading process"

    configure_environment
    generate_tpch_data
    load_to_iceberg

    log "TPCH data generation and loading completed successfully" "SUCCESS"
}

# Execute main function with logging
main 2>&1 | tee /home/iceberg/load_tpch.log
