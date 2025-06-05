# My Trackmania Map Analyzer

This is a personal Python project designed to help me analyze data for all the Trackmania maps I've built (currently around 40, but growing!). It uses the Trackmania.io API to fetch leaderboard information and provides a Tkinter GUI to explore various statistics.

## What it Does

*   **Fetches Map Data:** Connects to the Trackmania.io API to retrieve leaderboard data for maps specified in a local text file. This includes player times, ranks, and when records were set.
*   **Local Storage:** Saves all fetched data into an SQLite database for persistent storage and quick access.
*   **GUI for Analysis:** The Tkinter interface allows me to:
    *   View an overview of all tracked maps and players.
    *   See detailed leaderboards for each map.
    *   Track an overall leaderboard across all my maps using a points system.
    *   Look up individual players to see which of my maps they've played and their performance.
    *   Identify new Personal Bests and new players on maps since the last data fetch.
*   **Respectful API Usage:** The script includes delays to respect Trackmania.io's rate limits (around 100-200 requests per day typically, depending on how often I refresh).

## How it Works (General Idea)

1.  I maintain a list of my map API URLs in a `maps_api_urls.txt` file.
2.  When I run the script and hit the "Fetch/Refresh" button, it goes through each URL, calls the Trackmania.io API for the latest leaderboard data, and updates the local SQLite database.
3.  The GUI then reads from this database to display all the tables and player profiles.

This tool helps me see who's playing my maps, how competitive they are, and discover interesting player achievements and patterns. It's fascinating to see the data come alive!

## Regarding API Usage

I strive to use the Trackmania.io API respectfully and within its intended spirit for personal analysis of my own map data. If the maintainers or operators of Trackmania.io have any questions or concerns regarding my use of the API with this tool, please feel free to contact me on Discord: **themaster1127**

---

Powered by [Trackmania.io](https://trackmania.io/)"This is a new project called TrackmaniaMapInfoAnalyzer"