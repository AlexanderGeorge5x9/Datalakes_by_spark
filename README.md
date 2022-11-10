<div class="ltr"><div class="_15vzQlp3FJ8f94suLiPCPf ureact-markdown "><h1 id="schema-for-song-play-analysis">Schema for Song Play Analysis</h1>
<p>Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.</p>
<h4 id="fact-table">Fact Table</h4>
<ol>
<li><strong>songplays</strong> - records in log data associated with song plays i.e. records with page <code>NextSong</code>  <ul>
<li><em>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</em></li>
</ul>
</li>
</ol>
<h4 id="dimension-tables">Dimension Tables</h4>
<ol start="2">
<li><strong>users</strong> - users in the app<ul>
<li><em>user_id, first_name, last_name, gender, level</em></li>
</ul>
</li>
<li><strong>songs</strong> - songs in music database<ul>
<li><em>song_id, title, artist_id, year, duration</em></li>
</ul>
</li>
<li><strong>artists</strong> - artists in music database<ul>
<li><em>artist_id, name, location, lattitude, longitude</em></li>
</ul>
</li>
<li><strong>time</strong> - timestamps of records in <strong>songplays</strong> broken down into specific units<ul>
<li><em>start_time, hour, day, week, month, year, weekday</em></li>
</ul>
</li>
</ol>
</div></div>
<div><div class="index--container--2OwOl"><div class="index--atom--lmAIo layout--content--3Smmq"><div class="ltr"><div class="_15vzQlp3FJ8f94suLiPCPf ureact-markdown "><h1 id="project-template">Project Template</h1>
<p>To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project with a smaller dataset found in the workspace, and then move on to the bigger dataset on AWS.</p>
<p>Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.</p>
<p>The project template includes three files:</p>
<ul>
<li><code>etl.py</code> reads data from S3, processes that data using Spark, and writes them back to S3</li>
<li><code>dl.cfg</code>contains your AWS credentials</li>
<li><code>README.md</code> provides discussion on your process and decisions</li>
</ul>
</div></div><span></span></div><span></span></div></div>