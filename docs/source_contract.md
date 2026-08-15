# Spotify - Source Data Contract

## Project Scope

The Spotify Listening Analytics project analyzes personal music listening
behavior using data collected from the Spotify Web API.

The initial pipeline uses 3 Spotify data sources.

### Recently Played Tracks

**Endpoint:** `/me/player/recently-played`

**Purpose:** Primary source of listening activity.

**Grain:** One record per track play.

**Planned uses:**
- Listening history
- Listening sessions
- Artist and album consumption
- Repeat listening behavior
- Time-of-day analysis

### Top Items

**Endpoint:** `/me/top/{type}`

**Purpose:** Capture Spotify-calculated artist and track affinity over time.

**Grain:** One item per ranking position, time range, and extraction date.

**Time ranges:**
- Short term
- Medium term
- Long term

### Saved Tracks

**Endpoint:** `/me/tracks`

**Purpose:** Capture tracks added to the user's Spotify library.

**Grain:** One record per saved track.

**Planned uses:**
- Library growth
- Listen-to-save behavior
- Artist affinity
- Discovery analysis
