with albums as (

    select *
    from {{ source('spotify_silver', 'albums') }}

),

final as (

    select
        album_id,
        album_name,
        cast(album_release_date as date) as album_release_date,
        first_observed_at,
        last_observed_at

    from albums

)

select *
from final