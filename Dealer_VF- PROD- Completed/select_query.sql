CREATE TABLE
  CUID.VF_DEALER_TEMP (
    `PANNO` Nullable(String),
    `ACTIVE_STATUS` Nullable(String),
    `NAME_OF_MASTER` Nullable(String),
    `MAKEDATE` NULLABLE(String),
    `LASTUPDATED_NEW` NULLABLE(String),
    `TYPE_OF_BROKER` Nullable(String),
    `VF_BK_MASTER` Nullable(String),
    `checksum` UInt64 MATERIALIZED cityHash64(
      (
        coalesce(PANNO, ''),
        coalesce(ACTIVE_STATUS, ''),
        coalesce(NAME_OF_MASTER, ''),
        coalesce(MAKEDATE, ''),
        coalesce(TYPE_OF_BROKER, ''),
        coalesce(VF_BK_MASTER, '')
      )
    )
  ) ENGINE = MergeTree
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;


-- `CUID`.`VF_DEALER` definition
CREATE TABLE
  CUID.VF_DEALER (
    `PANNO` Nullable(String),
    `ACTIVE_STATUS` Nullable(String),
    `NAME_OF_MASTER` Nullable(String),
    `MAKEDATE` NULLABLE(String),
    `LASTUPDATED_NEW` NULLABLE(String),
    `TYPE_OF_BROKER` Nullable(String),
    `VF_BK_MASTER` Nullable(String),
    `checksum` UInt64,
    `version` UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    `LastUpdated` DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(version)
ORDER BY
  checksum SETTINGS allow_nullable_key = 1,
  index_granularity = 8192;