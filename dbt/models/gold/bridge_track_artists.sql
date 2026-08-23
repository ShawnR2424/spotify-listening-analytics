with track_artists as (

    select *
    from {{ source('spotify_silver', 'track_artists') }}

),

final as (

    select
        sha2(
            concat_ws(
                '||',
                track_id,
                artist_id
            ),
            256
        ) as track_artist_key,

        track_id,
        artist_id,
        artist_position

    from track_artists

)

select *
from final