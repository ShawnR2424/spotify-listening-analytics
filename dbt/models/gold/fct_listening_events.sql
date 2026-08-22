with listening_events as (

    select *
    from {{ source('spotify_silver', 'listening_events') }}

),

final as (

    select
        listening_event_id,

        played_at,
        cast(played_at as date) as played_date,

        track_id,
        album_id,

        duration_ms as track_duration_ms,
        is_explicit,

        context_type,
        context_uri,

        batch_id,
        ingested_at

    from listening_events

)

select *
from final