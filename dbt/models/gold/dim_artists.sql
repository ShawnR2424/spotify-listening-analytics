with artists as (

    select *
    from {{ source('spotify_silver', 'artists') }}

),

final as (

    select
        artist_id,
        artist_name,
        artist_uri,
        first_observed_at,
        last_observed_at

    from artists

)

select *
from final