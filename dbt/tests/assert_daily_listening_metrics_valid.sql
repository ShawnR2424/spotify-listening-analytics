select *

from {{ ref('mart_daily_listening') }}

where
       total_plays < 0
    or unique_tracks < 0
    or unique_artists < 0
    or listening_minutes < 0
    or repeat_plays < 0
    or explicit_plays < 0
    or repeat_plays > total_plays
    or unique_tracks > total_plays
    or repeat_play_rate < 0
    or repeat_play_rate > 1