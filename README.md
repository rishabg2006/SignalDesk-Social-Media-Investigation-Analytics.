# Signal Desk

The project includes `data/bluesky_madoc.parquet` (2,814,352 Bluesky posts).

To open the dashboard with the full dataset, run:

```powershell
python server.py
```

Then open <http://127.0.0.1:8765/>. The page reads the Parquet file through the local service so charts and filters use the full dataset without loading millions of post objects into the browser at once. The server binds to localhost only.

The dashboard includes a **Trends & ideas** page. It compares topic frequency over the latest 14 days in the dataset with the prior 14 days and can be narrowed to an account with the User filter. This file has no view or impression counts, so posts cannot be ranked by views; repeated repost text is shown as a separate, clearly labeled signal.

The Parquet file contains post IDs, Unix timestamps, user IDs, parent/reply metadata, post text, platform, interaction type, language, and sentiment. It does not contain views, impressions, likes, or shares.
