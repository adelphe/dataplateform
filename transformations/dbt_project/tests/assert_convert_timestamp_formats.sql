-- Test: validate that convert_timestamp correctly handles epoch seconds,
-- ISO 8601 strings, and pre-existing timestamp values.
-- This test fails if any row produces a mismatched result.

with test_cases as (

    select
        -- Epoch seconds (numeric): 2021-01-01 00:00:00 UTC
        {{ convert_timestamp('1609459200::bigint') }} as from_epoch_int,

        -- Epoch seconds (text): same date
        {{ convert_timestamp("'1609459200'") }} as from_epoch_string,

        -- ISO 8601 string (with T separator and Z suffix)
        {{ convert_timestamp("'2021-01-01T00:00:00Z'") }} as from_iso8601_z,

        -- ISO 8601 string (with space separator, no timezone)
        {{ convert_timestamp("'2021-01-01 00:00:00'") }} as from_iso8601_space,

        -- Already a timestamp value
        {{ convert_timestamp("timestamp '2021-01-01 00:00:00'") }} as from_timestamp,

        -- Fractional epoch (double precision)
        {{ convert_timestamp('1609459200.0::double precision') }} as from_epoch_float

),

expected as (

    select timestamp '2021-01-01 00:00:00' as expected_ts

)

select *
from test_cases, expected
where
    from_epoch_int      != expected_ts
    or from_epoch_string   != expected_ts
    or from_iso8601_z      != expected_ts
    or from_iso8601_space  != expected_ts
    or from_timestamp      != expected_ts
    or from_epoch_float    != expected_ts
