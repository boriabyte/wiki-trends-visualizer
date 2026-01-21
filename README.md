# Wikipedia Trends Visualizer

This visualizer concerns itself with creating an interactive dashboard that intuitively showcases daily Wikipedia trends.

The premise is simple: data is stored to https://dumps.wikimedia.org/other/pageviews/2026/2026-01/; it is fetched and processed
so it can be human-readable, yielding useful information relevant to this application.

The entire pipeline is already integrated in main.py. To run this application locally (it may take a while to download all the files, depending on WHEN you run it):

1)  click CTRL + F5 while on the main.py file to run it "conventionally"
2)  run 'python main.py' from the terminal while in the working directory after downloading the files locally

Sometimes, it may be necessary to run it multiple times as, due to the sheer size of the data fetched, it can fail at this stage (can also be due to some corruption of the .gz files). 
Depending on the machine it runs on, the day it runs on (having to fetch new files so it can be up-to-date), the time it takes to complete the entire process and also compile the dashboard is variable.
