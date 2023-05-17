# Video-Game-Discounts
Is that video game discount a good deal? What are the expected discount tendencies of an upcoming game? Use this consumer tool and find out. ... View historical trends of video game discounts in grouped format. Template for your own database. Sourced mainly from Steam and gg.deals. Link to database file (sqlite3): https://www.mediafire.com/file/fbnmm5aw2dojol2/game-price_db.sqlite/file

**Introduction:**

This is my first portfolio project for my professional data analytics journey. My analytical interest in the creation of this database was to identify whether a given discount for a pre-launch game was likely the best offer to occur before the game's release. This interest came to me when I missed out on an advantageous discount, seeing a more expensive lowest available price since. Part of the appeal of offering discounts is urgency. Though in contrast, when the discount sites are aggregated, often times when one discount expires, another takes its place--an ongoing price ceiling. Consumers have poor means of gauging the price efficiency of a specific discount price (whether a better one is likely to occur before a critical date, or in consideration of relative value). That's what this tool solves--a real world problem for consumers solved using available data.

The data sources are Steam, Steamcharts.com, and gg.deals. Steam is a popular video game digital distribution service and storefront, accounting for between 50 to 70% of all PC game downloads around the world. Steamcharts sources its data from Steam. Gg.deals aggregates the various discount stores which offer discounts for PC games and charts the lowest available price for a game 'key' (used to redeem the game/product), and distinguishes the discount store by whether or not it's officially sanctioned to be a key seller--though this generally has negligible meaning.
  
**Technical Overview:**
  
  There are four (4) python files used. In combination, there are around 1500 lines of code and blanks, with the display options comprising 500 of these lines.
For display I prefer Jupyter Notebook (vers. 6.5.4), though Spyder (vers. 5.4.3) has handy features which makes it preferable for the bulk of tasks. This product features combined data sources from web scraping and interaction with an api, which are iteratively uploaded to an SQLite3 database, with checks for duplication. There are sanity checks at every steps, as well as a generally automated experience, pulling links and storing them to use whenever next the program is run.

**Files:**

1. project_database_efficiency.py
2. sql_iter_update.py
3. df_to_sql.py (for demonstration purposes)
4. proj_display.ipynb
5. game_price_bi.pdf (the PowerBI file exported as pdf)
6. game_price_bi.pbix (PowerBI file)

**How to Use:**

If using my SQLite3 file, skip to 3 below.
1. Run project_database_efficiency.py.
- Specify the amount of games
2. Run sql_iter_update.py
3. Run proj_display.ipynb
- Make sure the database file is located in the proper directory.
- Install the necessary libraries as prompted.
- Enter desired month range in absolute values as prompted. Enter the number of games to consider in the analysis.
- If you are interested in the broad market, press enter (blank) when prompted for series name. Enter a series if you'd like to see the discount trends of the previous editions of a series (e.g. "Call of Duty").
- If you are interested in a specific game without containing to a series consideration, as of now it simply evaulates the history of discounts from the publisher. You can further specify and copy above code for your peticular interests. This is located at the end of the file, where it specifies Manor Lords as the default.
