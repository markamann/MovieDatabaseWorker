# MovieDatabaseWorker

## Background and Purpose
I am a software developer looking to expand my skill set and learn new things.  The purpose of this solution is to provide a medium in which I can experiment, explore and learn new web-based technologies.  It is also intended to be a code sample and demonstration of my proficiency with these various technologies.

I was looking for a project to use as a learning tool.  I am a movie fan.  So, I decided to develop an application that uses the MyAPIFilms Internet Movie Database API (https://www.myapifilms.com) to automatically populate my database with data from the Internet Movie Database< (http://www.imdb.com).

This application, MovieDatabaseWorker, is a Windows console application that can run continuously.  It is written in C# using the .NET Framework 4.6.1.  Once I seeded it with a movie, it uses the API to download and store the movie data including a cast list.  It then uses the movie's cast list to look up the filmographies of the cast and adds those movies to a queue for future processing.

In this way, the application is self-populating.  I have included various functionality that will enable the application to continue if interrupted or if an error occurs.

## Use

Please note, the code published in the above mentioned public repositories on my GitHub account is intended for code sample purposes only.  You may download the solutions for review purposes. However, the solutions will not compile or run due to the absence of several dependencies.  Key among those dependencies is a library containing connection information.  This library contains private information such as database connection strings, usernames and passwords, private API tokens, and other sensitive information.  For security reasons, I cannot make this library or its code available to the public.

