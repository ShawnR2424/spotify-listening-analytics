with listening_events as (

    select *
    from {{ ref('fct_listening_events') }}

),

tracks as (

    select *
    from {{ ref('dim_tracks') }}

),

track_artists as (

    select *
    from {{ ref('bridge_track_artists') }}

),

event_enriched as (

    select
        e.listening_event_id,
        e.played_at,
        e.played_date,
        e.track_id,

        t.duration_ms,
        t.is_explicit,

        row_number() over (
            partition by
                e.played_date,
                e.track_id
            order by e.played_at
        ) as track_play_number_for_day

    from listening_events e

    left join tracks t
        on e.track_id = t.track_id

),

daily_core as (

    select
        played_date,

        count(*) as total_plays,

        count(distinct track_id) as unique_tracks,

        round(
            sum(duration_ms) / 60000.0,
            2
        ) as listening_minutes,

        sum(
            case
                when track_play_number_for_day > 1
                    then 1
                else 0
            end
        ) as repeat_plays,

        round(
            sum(
                case
                    when track_play_number_for_day > 1
                        then 1
                    else 0
                end
            ) / cast(count(*) as double),
            4
        ) as repeat_play_rate,

        sum(
            case
                when is_explicit then 1
                else 0
            end
        ) as explicit_plays,

        min(played_at) as first_played_at,
        max(played_at) as last_played_at

    from event_enriched

    group by played_date

),

daily_artists as (

    select
        e.played_date,

        count(
            distinct ta.artist_id
        ) as unique_artists

    from listening_events e

    inner join track_artists ta
        on e.track_id = ta.track_id

    group by e.played_date

),

final as (

    select
        d.played_date,

        d.total_plays,
        d.unique_tracks,
        coalesce(a.unique_artists, 0) as unique_artists,

        d.listening_minutes,

        d.repeat_plays,
        d.repeat_play_rate,

        d.explicit_plays,

        d.first_played_at,
        d.last_played_at

    from daily_core d

    left join daily_artists a
        on d.played_date = a.played_date

)

select *
from final