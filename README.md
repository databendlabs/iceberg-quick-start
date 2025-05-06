# Quick Start databend with iceberg rest catalog

## Deploy databend from official docs

## Start current docker compose

```
docker-compose up -d
```

## Load data via spark shell

- Entering the container:

```shell
docker exec spark-iceberg bash /home/iceberg/load_tpch.sh
```

## Let's query data from databend

- create catalog in bendsql
```
:) CREATE CATALOG iceberg TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/wh/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);

```

- Query data

```
:) use catalog iceberg;
:) show databases;

╭──────────────────────╮
│ databases_in_iceberg │
│        String        │
├──────────────────────┤
│ tpch                 │
╰──────────────────────╯

:) select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    iceberg.tpch.lineitem
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│   l_returnflag   │   l_linestatus   │      sum_qty     │  sum_base_price  │  sum_disc_price  │    sum_charge    │      avg_qty     │     avg_price    │     avg_disc    │ count_order │
│ Nullable(String) │ Nullable(String) │ Nullable(Decimal │ Nullable(Decimal │ Nullable(Decimal │ Nullable(Decimal │ Nullable(Decimal │ Nullable(Decimal │ Nullable(Decima │    UInt64   │
│                  │                  │     (38, 2))     │     (38, 2))     │     (38, 4))     │     (38, 6))     │     (38, 8))     │     (38, 8))     │    l(38, 8))    │             │
├──────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────────┼─────────────┤
│ A                │ F                │      37734107.00 │   56586554400.73 │ 53758257134.8700 │ 55909065222.8276 │      25.52200585 │   38273.12973462 │      0.04998530 │     1478493 │
│                  │                  │                  │                  │                  │               92 │                  │                  │                 │             │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```
