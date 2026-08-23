with tracks as (

    select *
    from {{ source('spotify_silver', 'tracks') }}

),

final as (

    select
        track_id,
        track_uri,
        track_name,
        duration_ms,
        is_explicit,
        album_id,
        first_observed_at,
        last_observed_at

    from tracks

)

select *
from final