The solution for the Insight Data Engineering coding challenge is written in Python. The data are analyzed row by row. A set of lists track the information about the most recent session for each ip (start and end times and rows, pages visited and page counts, index of each ip). A separate set of lists tracks information about each session. The ip-level lists and session-level lists are updated with information from new rows, depending on three conditions: 1) an ip being observed that has not been seen before, 2) an ip already observed seen again in the same session, and 3) an ip observed again in a new session (based on elapsed time being greater than threshold). One area I would want to improve would be using a data structure other than lists, possibly dicts, to track all the ip- and session-level information, to reduce the number of variables. 

After processing the rows, it was necessary to sort the results by start and end times, to achieve the appropriate ordering. A second area I would work to improve would be changing the algorithm to avoid the need for this after-the-fact reordering. 

The code was tested with a second dataset from EDGAR. 

