#!/bin/bash

# Enable debugging
set -x

# Logging function with timestamp
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

# Error handling function
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Check required commands
check_commands() {
    local commands=("spark-shell")
    for cmd in "${commands[@]}"; do
        command -v "$cmd" >/dev/null 2>&1 || error_exit "Command not found: $cmd"
    done
}


# Main script execution
main() {
    log "Starting TPCH data generation and loading process"

    # Verify commands and environment
    check_commands

    # Set the scale factor for TPCH data generation
    SCALE_FACTOR=1
    log "Using TPCH scale factor: $SCALE_FACTOR"

    # Ensure data directory exists
    DATA_DIR="/home/iceberg/data/tpch_${SCALE_FACTOR}"
    log "Creating data directory: $DATA_DIR"
    mkdir -p "$DATA_DIR" || error_exit "Failed to create data directory"

    # Generate TPCH data using DuckDB
    log "Generating TPCH data with DuckDB"
    if ! command -v duckdb >/dev/null 2>&1; then
        log "DuckDB not found, installing..."
        curl https://install.duckdb.org | sh
        export PATH='/root/.duckdb/cli/latest':$PATH
    else
        log "DuckDB already installed, skipping..."
    fi
    duckdb << EOF || error_exit "DuckDB data generation failed"
install tpch;
load tpch;
CALL dbgen(sf = $SCALE_FACTOR);
EXPORT DATABASE '$DATA_DIR' (FORMAT PARQUET);
EOF

    # Verify data generation
    log "Checking generated data files"
    [ "$(find "$DATA_DIR" -name "*.parquet" | wc -l)" -eq 0 ] && error_exit "No Parquet files generated"

    # Load data into Iceberg catalog using Spark
    log "Loading data into Iceberg catalog"
    spark-shell --driver-memory 8g << EOF
import org.apache.spark.sql.SaveMode
import java.io.File

val icebergWarehouse = "s3://warehouse"
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

    log "TPCH data generation and loading completed successfully"
}

# Execute main function with error trapping
main 2>&1 | tee /home/iceberg/load_tpch.log
